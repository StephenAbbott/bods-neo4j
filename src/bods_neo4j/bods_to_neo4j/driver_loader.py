"""Load BODS data directly into Neo4j via the Python driver.

This approach is ideal for:
- Moderate datasets (up to ~1M statements)
- Incremental updates to an existing graph
- Development and testing workflows
- When you want immediate feedback and validation

For very large datasets (>1M statements), use the CSV export + neo4j-admin import instead.

The loader consumes the graph-native ``StatementGraph`` produced by
``bods_to_neo4j.mapper.map_statement`` and writes nodes/edges in a fixed order:

  Pass 0 (single file scan, before Neo4j is touched):
    Build a filter that identifies superseded statements:
      - latest ``statementId`` per relationship ``recordId``
        (highest ``statementDate``, lexicographic tiebreak), and
      - every ``statementId`` named in some other statement's
        ``replacesStatements`` (top-level or inside ``recordDetails``).
    Losers are skipped during Pass 1 / Pass 2 so they never reach Neo4j —
    no post-hoc delete pass, no piled-up family edges.
  Pass 1 (per chunk):
    1. Child nodes (Country, Identifier, Address) — MERGE by their dedup key.
    2. Primary nodes (Entity, Person) — MERGE by recordId.
    3. UnspecifiedParty sentinels — MERGE by uid (only when statements have
       inline unspecified subject / interestedParty).
  Sub-label fixups for entity variants (Trust, RegisteredEntity, etc.) run
  once between passes.
  Pass 2 (per chunk):
    1. Structural edges (HAS_IDENTIFIER, HAS_ADDRESS, LOCATED_IN,
       REGISTERED_IN, BORN_IN).
    2. Typed-family edges (OWNS / CONTROLS / MANAGES / IS_PARTY_TO /
       HAS_OTHER_INTEREST) directly between party and subject endpoints.
       Each edge carries the BODS interest payload + statement-level metadata.

``replacesStatements`` is used by the Pass 0 filter to drop superseded
statements, and the surviving statement's combined list is also stamped as
a passthrough property on the new node / typed edges so it round-trips back
through ``neo4j_to_bods``. No ``:REPLACES`` edge type exists in the graph;
the array is metadata only.

Caveat — incremental loads: the Pass 0 filter only sees the current file.
Re-running against a graph that already contains prior statements
(``clear_existing=False``) leaves any prior-load state untouched. Use
``--clear`` for a clean reload, or run a separate maintenance script to
evict stale data from a prior load.
"""

import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Union

from ..config import Neo4jConfig, ExportConfig
from ..utils.bods_schema import FAMILY_REL_TYPES
from ..utils.dedup import is_loser, scan_for_dedup
from ..utils.neo4j_helpers import (
    neo4j_driver,
    create_constraints,
    create_indexes,
    create_fulltext_index,
    clear_database,
    get_database_stats,
)
from .mapper import map_statement
from .reader import read_bods_file

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cypher queries: nodes
# ---------------------------------------------------------------------------

MERGE_COUNTRY_QUERY = """
UNWIND $batch AS row
MERGE (c:Country {code: row.key_value})
SET c += row.properties
"""

MERGE_IDENTIFIER_QUERY = """
UNWIND $batch AS row
MERGE (i:Identifier {uid: row.key_value})
SET i += row.properties
"""

MERGE_ADDRESS_QUERY = """
UNWIND $batch AS row
MERGE (a:Address {uid: row.key_value})
SET a += row.properties
"""

MERGE_ENTITY_QUERY = """
UNWIND $batch AS row
MERGE (e:Entity {recordId: row.key_value})
SET e += row.properties
"""

MERGE_PERSON_QUERY = """
UNWIND $batch AS row
MERGE (p:Person {recordId: row.key_value})
SET p += row.properties
"""

MERGE_UNSPECIFIED_PARTY_QUERY = """
UNWIND $batch AS row
MERGE (u:UnspecifiedParty {uid: row.key_value})
SET u += row.properties
"""


# ---------------------------------------------------------------------------
# Cypher queries: edges. Each query is keyed by start/end label pattern so
# they can be batched cleanly. Where the endpoint is :Entity OR :Person
# (party / subject), we COALESCE the two MATCHes.
# ---------------------------------------------------------------------------

# Entity/Person -> Identifier
CREATE_HAS_IDENTIFIER_QUERY = """
UNWIND $batch AS row
OPTIONAL MATCH (e:Entity {recordId: row.start_key_value})
OPTIONAL MATCH (p:Person {recordId: row.start_key_value})
WITH COALESCE(e, p) AS start, row
WHERE start IS NOT NULL
MATCH (i:Identifier {uid: row.end_key_value})
MERGE (start)-[r:HAS_IDENTIFIER]->(i)
SET r += row.properties
"""

# Entity/Person -> Address
CREATE_HAS_ADDRESS_QUERY = """
UNWIND $batch AS row
OPTIONAL MATCH (e:Entity {recordId: row.start_key_value})
OPTIONAL MATCH (p:Person {recordId: row.start_key_value})
WITH COALESCE(e, p) AS start, row
WHERE start IS NOT NULL
MATCH (a:Address {uid: row.end_key_value})
MERGE (start)-[r:HAS_ADDRESS {type: row.properties.type, ordinal: row.properties.ordinal}]->(a)
SET r += row.properties
"""

# Address -> Country
CREATE_LOCATED_IN_QUERY = """
UNWIND $batch AS row
MATCH (a:Address {uid: row.start_key_value})
MATCH (c:Country {code: row.end_key_value})
MERGE (a)-[r:LOCATED_IN]->(c)
"""

# Entity -> Country
CREATE_REGISTERED_IN_QUERY = """
UNWIND $batch AS row
MATCH (e:Entity {recordId: row.start_key_value})
MATCH (c:Country {code: row.end_key_value})
MERGE (e)-[r:REGISTERED_IN]->(c)
SET r += row.properties
"""

# Person -> Address (place of birth)
CREATE_BORN_IN_QUERY = """
UNWIND $batch AS row
MATCH (p:Person {recordId: row.start_key_value})
MATCH (a:Address {uid: row.end_key_value})
MERGE (p)-[r:BORN_IN]->(a)
"""

# Typed family edges go directly between party and subject. Each endpoint
# can be :Entity, :Person, or (for inline-unspecified) :UnspecifiedParty,
# so we try all three labels via OPTIONAL MATCH + COALESCE.
#
# Cypher relationship types can't be parameterised at runtime, so we
# render one query per family. Multiple BODS interests in a single
# relationship statement become multiple parallel edges between the same
# (party, subject) pair — we CREATE rather than MERGE so each (statementId,
# interestIndex) is preserved as a distinct edge.
_PARTY_TO_SUBJECT_TEMPLATE = """
UNWIND $batch AS row
OPTIONAL MATCH (e_s:Entity {recordId: row.start_key_value})
OPTIONAL MATCH (p_s:Person {recordId: row.start_key_value})
OPTIONAL MATCH (u_s:UnspecifiedParty {uid: row.start_key_value})
WITH row, COALESCE(e_s, p_s, u_s) AS start
OPTIONAL MATCH (e_t:Entity {recordId: row.end_key_value})
OPTIONAL MATCH (p_t:Person {recordId: row.end_key_value})
OPTIONAL MATCH (u_t:UnspecifiedParty {uid: row.end_key_value})
WITH row, start, COALESCE(e_t, p_t, u_t) AS target
WHERE start IS NOT NULL AND target IS NOT NULL
CREATE (start)-[r:%s]->(target)
SET r += row.properties
"""

# Sub-label fixups (run after primary nodes are loaded). Same idea as the
# original loader but extended for Interest families.
SET_ENTITY_SUBTYPE_LABELS = [
    "MATCH (e:Entity) WHERE e.entityType = 'registeredEntity' SET e:RegisteredEntity",
    "MATCH (e:Entity) WHERE e.entityType = 'legalEntity' SET e:LegalEntity",
    "MATCH (e:Entity) WHERE e.entityType = 'arrangement' SET e:Arrangement",
    "MATCH (e:Entity) WHERE e.entityType = 'anonymousEntity' SET e:AnonymousEntity",
    "MATCH (e:Entity) WHERE e.entityType = 'unknownEntity' SET e:UnknownEntity",
    "MATCH (e:Entity) WHERE e.entityType = 'state' SET e:State",
    "MATCH (e:Entity) WHERE e.entityType = 'stateBody' SET e:StateBody",
    "MATCH (e:Entity) WHERE e.entitySubtype = 'trust' SET e:Trust",
    "MATCH (e:Entity) WHERE e.entitySubtype = 'nomination' SET e:Nomination",
    "MATCH (e:Entity) WHERE e.entitySubtype = 'governmentDepartment' SET e:GovernmentDepartment",
    "MATCH (e:Entity) WHERE e.entitySubtype = 'stateAgency' SET e:StateAgency",
]



# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


_NODE_LABEL_QUERIES = [
    ("Country", MERGE_COUNTRY_QUERY),
    ("Identifier", MERGE_IDENTIFIER_QUERY),
    ("Address", MERGE_ADDRESS_QUERY),
    ("Entity", MERGE_ENTITY_QUERY),
    ("Person", MERGE_PERSON_QUERY),
    ("UnspecifiedParty", MERGE_UNSPECIFIED_PARTY_QUERY),
]

_STRUCTURAL_EDGE_QUERIES = [
    ("HAS_IDENTIFIER", CREATE_HAS_IDENTIFIER_QUERY),
    ("HAS_ADDRESS", CREATE_HAS_ADDRESS_QUERY),
    ("LOCATED_IN", CREATE_LOCATED_IN_QUERY),
    ("REGISTERED_IN", CREATE_REGISTERED_IN_QUERY),
    ("BORN_IN", CREATE_BORN_IN_QUERY),
]


def load_bods_to_neo4j(
    bods_file: Union[str, Path],
    neo4j_config: Neo4jConfig = None,
    export_config: ExportConfig = None,
) -> dict:
    """Load BODS data from a file directly into Neo4j.

    Memory-bounded streaming load: one pre-scan + two streaming passes.

        Pass 0: read the BODS file once to build a ``PreScan`` —
                the winning ``statementId`` per relationship ``recordId``
                (latest by ``statementDate``, tiebreak lexicographic) and
                the set of ``statementId``s named in any other statement's
                ``replacesStatements`` array. Cheap, O(N) in Python.

        Pass 1: forward-map and flush nodes (every ``chunk_size`` statements).
                Apply entity-subtype labels.

        Pass 2: forward-map and flush edges (every ``chunk_size`` statements).

    During Pass 1 + Pass 2 the loader **skips losers** identified by the
    pre-scan: any relationship statement that isn't the latest for its
    ``recordId``, and any statement whose ``statementId`` is named in some
    other statement's ``replacesStatements``. That way superseded data
    never reaches Neo4j — no post-hoc Cypher delete pass, no piled-up
    family edges to clean up.

    Caveat: the pre-scan only sees the current file. Incremental loads
    (``clear_existing=False`` on a graph that already contains prior
    statements) will leave the older graph state in place. Re-run with
    ``--clear`` (or a separate maintenance script) to evict stale data
    from a prior load.

    Args:
        bods_file: Path to BODS JSON or JSONL file
        neo4j_config: Neo4j connection configuration
        export_config: Export configuration options

    Returns:
        Dictionary with counts and statistics
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    if export_config is None:
        export_config = ExportConfig()

    counts = {
        "entity_statements": 0,
        "person_statements": 0,
        "relationship_statements": 0,
        "skipped": 0,
        "skipped_superseded": 0,
    }

    chunk_size = max(getattr(export_config, "streaming_chunk_size", 0)
                     or export_config.batch_size * 10, 5000)

    logger.info("Pass 0: scanning %s for latest relationship versions and "
                "superseded statementIds", bods_file)
    pre_scan = scan_for_dedup(bods_file)
    logger.info(
        "Pre-scan: %d unique relationship recordIds, %d statementIds "
        "named in replacesStatements",
        len(pre_scan.latest_statement_per_record),
        len(pre_scan.superseded_statement_ids),
    )

    with neo4j_driver(neo4j_config) as driver:
        database = neo4j_config.database

        if export_config.clear_existing:
            clear_database(driver, database)
        if export_config.create_schema:
            create_constraints(driver, database)
            create_indexes(driver, database)

        logger.info("Pass 1: streaming nodes (chunk size=%d) from %s",
                    chunk_size, bods_file)
        # node_buckets: { label -> { key -> spec } }  (dedup within chunk)
        node_buckets = defaultdict(dict)
        in_flight = 0
        for statement in read_bods_file(bods_file):
            if is_loser(statement, pre_scan):
                counts["skipped_superseded"] += 1
                continue
            graph = map_statement(statement)
            if graph is None:
                counts["skipped"] += 1
                continue
            statement_type = graph["statement_type"]
            counts[f"{statement_type}_statements"] += 1

            for node_spec in graph["nodes"]:
                label = node_spec["labels"][0]
                key = node_spec["key_value"]
                existing = node_buckets[label].get(key)
                if existing is None:
                    node_buckets[label][key] = node_spec
                    in_flight += 1
                else:
                    merged = dict(existing["properties"])
                    for k, v in node_spec["properties"].items():
                        if v not in (None, ""):
                            merged[k] = v
                    existing["properties"] = merged

            if in_flight >= chunk_size:
                _flush_node_chunk(driver, database, node_buckets, export_config)
                node_buckets = defaultdict(dict)
                in_flight = 0

        if in_flight:
            _flush_node_chunk(driver, database, node_buckets, export_config)

        if export_config.use_subtype_labels:
            logger.info("Applying entity subtype labels")
            for q in SET_ENTITY_SUBTYPE_LABELS:
                try:
                    driver.execute_query(q, database_=database)
                except Exception as e:  # pragma: no cover - defensive
                    logger.debug("Sub-label query failed: %s | %s", q, e)

        logger.info("Pass 2: streaming edges (chunk size=%d) from %s",
                    chunk_size, bods_file)
        # edge_buckets: { rel_type -> [ spec, ... ] }
        edge_buckets = defaultdict(list)
        in_flight = 0
        for statement in read_bods_file(bods_file):
            if is_loser(statement, pre_scan):
                continue
            graph = map_statement(statement)
            if graph is None:
                continue
            # Optional cosmetic flag — every loaded relationship edge is
            # by definition the latest version (losers were filtered in
            # Pass 0); set the property so downstream consumers expecting
            # it keep working.
            stamp_latest = (
                export_config.stamp_latest_versions
                and graph["statement_type"] == "relationship"
            )
            for edge_spec in graph["edges"]:
                if stamp_latest and edge_spec["rel_type"] in FAMILY_REL_TYPES:
                    edge_spec["properties"]["isLatestVersion"] = True
                edge_buckets[edge_spec["rel_type"]].append(edge_spec)
                in_flight += 1
            if in_flight >= chunk_size:
                _flush_edge_chunk(driver, database, edge_buckets, export_config)
                edge_buckets = defaultdict(list)
                in_flight = 0

        if in_flight:
            _flush_edge_chunk(driver, database, edge_buckets, export_config)

        if export_config.create_schema:
            create_fulltext_index(driver, database)

        counts["db_stats"] = get_database_stats(driver, database)

    logger.info(
        "Load complete: %d entity / %d person / %d relationship statements "
        "(%d superseded skipped, %d malformed skipped)",
        counts["entity_statements"], counts["person_statements"],
        counts["relationship_statements"],
        counts["skipped_superseded"], counts["skipped"],
    )
    return counts


def _flush_node_chunk(driver, database, node_buckets, export_config):
    """Flush one chunk of node buckets in dependency order."""
    for label, query in _NODE_LABEL_QUERIES:
        _flush_nodes(driver, database, label, query, node_buckets, export_config)


def _flush_edge_chunk(driver, database, edge_buckets, export_config):
    """Flush one chunk of edge buckets, structural edges first then families."""
    for rel_type, query in _STRUCTURAL_EDGE_QUERIES:
        _flush_edges(driver, database, rel_type, query, edge_buckets, export_config)
    for family in FAMILY_REL_TYPES:
        _flush_edges(
            driver, database, family,
            _PARTY_TO_SUBJECT_TEMPLATE % family,
            edge_buckets, export_config,
        )


# ---------------------------------------------------------------------------
# Internal flush helpers
# ---------------------------------------------------------------------------


def _flush_nodes(driver, database, label, query, node_buckets, export_config):
    specs = list(node_buckets.get(label, {}).values())
    if not specs:
        return
    batch = [
        {"key_value": s["key_value"], "properties": _serialise_props(s["properties"])}
        for s in specs
    ]
    _execute_batches(driver, database, query, batch, export_config.batch_size,
                     description=f"nodes[{label}]")


def _flush_edges(driver, database, rel_type, query, edge_buckets, export_config):
    specs = edge_buckets.get(rel_type, [])
    if not specs:
        return
    batch = [
        {
            "start_key_value": s["start_key_value"],
            "end_key_value": s["end_key_value"],
            "properties": _serialise_props(s["properties"]),
        }
        for s in specs
    ]
    _execute_batches(driver, database, query, batch, export_config.batch_size,
                     description=f"edges[{rel_type}]")


def _execute_batches(driver, database, query, batch, batch_size, description):
    if not batch:
        return
    total = 0
    for i in range(0, len(batch), batch_size):
        chunk = batch[i : i + batch_size]
        driver.execute_query(query, parameters_={"batch": chunk}, database_=database)
        total += len(chunk)
    logger.info("Flushed %d %s", total, description)


def _serialise_props(props: dict) -> dict:
    """Neo4j accepts primitives, booleans, and lists of primitives — but not
    nested dicts. Anything we couldn't flatten ended up under a ``*Json`` key
    already, so we just pass through here."""
    return {k: v for k, v in props.items() if v is not None}
