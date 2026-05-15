"""Export BODS data to Neo4j-importable CSV files with an import script.

Produces one CSV per node label and one CSV per relationship type, matching
the graph-native shape emitted by ``bods_to_neo4j.mapper``:

    Nodes:
        country.csv          (:Country)
        identifier.csv       (:Identifier)
        address.csv          (:Address)
        entity.csv           (:Entity + subtype labels via post-load fixup)
        person.csv           (:Person)
        interest.csv         (:Interest + family sub-label via post-load fixup)

    Relationships:
        has_identifier.csv, has_address.csv, located_in.csv,
        registered_in.csv, born_in.csv, in.csv,
        owns.csv, controls.csv, manages.csv, is_party_to.csv,
        has_other_interest.csv

    Scripts:
        import.cypher    LOAD CSV script for a running Neo4j instance
        import.sh        Convenience runner via cypher-shell
"""

import csv
import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterable, Union

from ..utils.bods_schema import FAMILY_REL_TYPES
from ..utils.dedup import is_loser, scan_for_dedup
from .mapper import map_statement
from .reader import read_bods_file

logger = logging.getLogger(__name__)


_NODE_LABELS = ["Country", "Identifier", "Address", "Entity", "Person", "UnspecifiedParty"]
_STRUCTURAL_REL_TYPES = [
    "HAS_IDENTIFIER", "HAS_ADDRESS", "LOCATED_IN", "REGISTERED_IN",
    "BORN_IN", "COMPONENT_OF",
]
_ALL_REL_TYPES = _STRUCTURAL_REL_TYPES + FAMILY_REL_TYPES


_LABEL_TO_FILENAME = {
    "Country": "country.csv",
    "Identifier": "identifier.csv",
    "Address": "address.csv",
    "Entity": "entity.csv",
    "Person": "person.csv",
    "UnspecifiedParty": "unspecified_party.csv",
}


def export_to_csv(
    bods_file: Union[str, Path],
    output_dir: Union[str, Path] = "./neo4j_export",
) -> dict:
    """Export BODS data to per-label / per-rel-type CSV files.

    Returns counts of statements and total nodes / edges written.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    counts = {
        "entity_statements": 0,
        "person_statements": 0,
        "relationship_statements": 0,
        "skipped": 0,
        "skipped_superseded": 0,
    }
    nodes_by_label: dict[str, dict] = {label: {} for label in _NODE_LABELS}
    edges_by_type: dict[str, list] = defaultdict(list)

    logger.info("Pass 0: scanning %s for latest relationship versions and "
                "superseded statementIds", bods_file)
    pre_scan = scan_for_dedup(bods_file)
    logger.info(
        "Pre-scan: %d unique relationship recordIds, %d superseded statementIds",
        len(pre_scan.latest_statement_per_record),
        len(pre_scan.superseded_statement_ids),
    )

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

        for node in graph["nodes"]:
            label = node["labels"][0]
            bucket = nodes_by_label.setdefault(label, {})
            key = node["key_value"]
            existing = bucket.get(key)
            if existing is None:
                bucket[key] = node
            else:
                merged = dict(existing["properties"])
                for k, v in node["properties"].items():
                    if v not in (None, ""):
                        merged[k] = v
                existing["properties"] = merged

        for edge in graph["edges"]:
            edges_by_type[edge["rel_type"]].append(edge)

    node_totals = _write_node_csvs(output_dir, nodes_by_label)
    edge_totals = _write_edge_csvs(output_dir, edges_by_type)
    _generate_import_cypher(output_dir, node_totals, edge_totals)
    _generate_import_sh(output_dir)

    counts["nodes"] = node_totals
    counts["edges"] = edge_totals
    logger.info(
        "Export complete: %s entity / %s person / %s relationship statements "
        "(%s superseded skipped, %s malformed skipped)",
        counts["entity_statements"], counts["person_statements"],
        counts["relationship_statements"],
        counts["skipped_superseded"], counts["skipped"],
    )
    return counts


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------


def _write_node_csvs(output_dir: Path, nodes_by_label: dict) -> dict:
    totals = {}
    for label, bucket in nodes_by_label.items():
        rows = [_flatten_for_csv(n["properties"]) for n in bucket.values()]
        if not rows:
            continue
        columns = _union_columns(rows)
        path = output_dir / _LABEL_TO_FILENAME.get(label, f"{label.lower()}.csv")
        _write_csv(path, columns, rows)
        totals[label] = len(rows)
    return totals


def _write_edge_csvs(output_dir: Path, edges_by_type: dict) -> dict:
    totals = {}
    for rel_type in _ALL_REL_TYPES:
        edges = edges_by_type.get(rel_type, [])
        if not edges:
            continue
        rows = [
            {
                "start_key": e["start_key_value"],
                "end_key": e["end_key_value"],
                **_flatten_for_csv(e["properties"]),
            }
            for e in edges
        ]
        columns = _union_columns(rows)
        path = output_dir / f"{rel_type.lower()}.csv"
        _write_csv(path, columns, rows)
        totals[rel_type] = len(rows)
    return totals


def _write_csv(path: Path, columns: list, rows: list) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.debug("Wrote %d rows to %s", len(rows), path)


def _flatten_for_csv(props: dict) -> dict:
    """Lists become JSON-encoded strings so CSV stays single-valued per cell.
    Cypher's LOAD CSV step parses these back via apoc.convert.fromJsonList()."""
    out = {}
    for k, v in props.items():
        if isinstance(v, list):
            out[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif v is None:
            out[k] = ""
        else:
            out[k] = v
    return out


def _union_columns(rows: list) -> list:
    cols = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                cols.append(k)
                seen.add(k)
    return cols


# ---------------------------------------------------------------------------
# Import scripts
# ---------------------------------------------------------------------------


_PARTY_TO_SUBJECT_LOAD_TPL = """
LOAD CSV WITH HEADERS FROM 'file:///%s' AS row
CALL {
    WITH row
    OPTIONAL MATCH (e_s:Entity {recordId: row.start_key})
    OPTIONAL MATCH (p_s:Person {recordId: row.start_key})
    OPTIONAL MATCH (u_s:UnspecifiedParty {uid: row.start_key})
    WITH row, COALESCE(e_s, p_s, u_s) AS start
    OPTIONAL MATCH (e_t:Entity {recordId: row.end_key})
    OPTIONAL MATCH (p_t:Person {recordId: row.end_key})
    OPTIONAL MATCH (u_t:UnspecifiedParty {uid: row.end_key})
    WITH row, start, COALESCE(e_t, p_t, u_t) AS target
    WHERE start IS NOT NULL AND target IS NOT NULL
    CREATE (start)-[r:%s]->(target)
    SET r += row
} IN TRANSACTIONS OF 10000 ROWS;
"""


def _generate_import_cypher(output_dir: Path, node_totals: dict, edge_totals: dict) -> None:
    sections: list[str] = []
    sections.append(_HEADER)
    sections.append(_CONSTRAINTS_BLOCK)

    if node_totals.get("Country"):
        sections.append(_render_load_node("country.csv", "Country", "code"))
    if node_totals.get("Identifier"):
        sections.append(_render_load_node("identifier.csv", "Identifier", "uid"))
    if node_totals.get("Address"):
        sections.append(_render_load_node("address.csv", "Address", "uid"))
    if node_totals.get("Entity"):
        sections.append(_render_load_node("entity.csv", "Entity", "recordId"))
        sections.append(_ENTITY_SUBTYPE_FIXUP)
    if node_totals.get("Person"):
        sections.append(_render_load_node("person.csv", "Person", "recordId"))
    if node_totals.get("UnspecifiedParty"):
        sections.append(_render_load_node("unspecified_party.csv", "UnspecifiedParty", "uid"))

    if edge_totals.get("HAS_IDENTIFIER"):
        sections.append(_HAS_IDENTIFIER_LOAD)
    if edge_totals.get("HAS_ADDRESS"):
        sections.append(_HAS_ADDRESS_LOAD)
    if edge_totals.get("LOCATED_IN"):
        sections.append(_LOCATED_IN_LOAD)
    if edge_totals.get("REGISTERED_IN"):
        sections.append(_REGISTERED_IN_LOAD)
    if edge_totals.get("BORN_IN"):
        sections.append(_BORN_IN_LOAD)
    for family in FAMILY_REL_TYPES:
        if edge_totals.get(family):
            sections.append(_PARTY_TO_SUBJECT_LOAD_TPL % (f"{family.lower()}.csv", family))

    sections.append(_FULLTEXT_INDEX)
    sections.append(_SUMMARY_QUERY)

    script_path = output_dir / "import.cypher"
    script_path.write_text("\n".join(sections), encoding="utf-8")
    logger.info("Generated import script: %s", script_path)


def _generate_import_sh(output_dir: Path) -> None:
    script = """#!/bin/bash
# Run the import.cypher script via cypher-shell against a running Neo4j instance.
# Place the CSV files in Neo4j's import directory (or serve them via HTTP) first.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cypher-shell "${@}" < "${DIR}/import.cypher"
"""
    path = output_dir / "import.sh"
    path.write_text(script, encoding="utf-8")
    path.chmod(0o755)


# ---------------------------------------------------------------------------
# Cypher templates
# ---------------------------------------------------------------------------


_HEADER = """// BODS Neo4j Import Script
// Generated by bods-neo4j (graph-native schema)
// Run from Neo4j Browser or cypher-shell with CSVs in the import directory.
"""

_CONSTRAINTS_BLOCK = """
CREATE CONSTRAINT constraint_entity_record_id IF NOT EXISTS
FOR (e:Entity) REQUIRE e.recordId IS UNIQUE;
CREATE CONSTRAINT constraint_person_record_id IF NOT EXISTS
FOR (p:Person) REQUIRE p.recordId IS UNIQUE;
CREATE CONSTRAINT constraint_identifier_uid IF NOT EXISTS
FOR (i:Identifier) REQUIRE i.uid IS UNIQUE;
CREATE CONSTRAINT constraint_address_uid IF NOT EXISTS
FOR (a:Address) REQUIRE a.uid IS UNIQUE;
CREATE CONSTRAINT constraint_country_code IF NOT EXISTS
FOR (c:Country) REQUIRE c.code IS UNIQUE;
CREATE CONSTRAINT constraint_unspecified_party_uid IF NOT EXISTS
FOR (u:UnspecifiedParty) REQUIRE u.uid IS UNIQUE;
"""

def _render_load_node(filename: str, label: str, key_prop: str) -> str:
    return (
        f"\nLOAD CSV WITH HEADERS FROM 'file:///{filename}' AS row\n"
        "CALL {\n"
        "    WITH row\n"
        f"    MERGE (n:{label} {{{key_prop}: row.{key_prop}}})\n"
        "    SET n += row\n"
        "} IN TRANSACTIONS OF 10000 ROWS;\n"
    )

_ENTITY_SUBTYPE_FIXUP = """
MATCH (e:Entity) WHERE e.entityType = 'registeredEntity' SET e:RegisteredEntity;
MATCH (e:Entity) WHERE e.entityType = 'legalEntity' SET e:LegalEntity;
MATCH (e:Entity) WHERE e.entityType = 'arrangement' SET e:Arrangement;
MATCH (e:Entity) WHERE e.entityType = 'anonymousEntity' SET e:AnonymousEntity;
MATCH (e:Entity) WHERE e.entityType = 'unknownEntity' SET e:UnknownEntity;
MATCH (e:Entity) WHERE e.entityType = 'state' SET e:State;
MATCH (e:Entity) WHERE e.entityType = 'stateBody' SET e:StateBody;
MATCH (e:Entity) WHERE e.entitySubtype = 'trust' SET e:Trust;
MATCH (e:Entity) WHERE e.entitySubtype = 'nomination' SET e:Nomination;
MATCH (e:Entity) WHERE e.entitySubtype = 'governmentDepartment' SET e:GovernmentDepartment;
MATCH (e:Entity) WHERE e.entitySubtype = 'stateAgency' SET e:StateAgency;
"""

_HAS_IDENTIFIER_LOAD = """
LOAD CSV WITH HEADERS FROM 'file:///has_identifier.csv' AS row
CALL {
    WITH row
    OPTIONAL MATCH (e:Entity {recordId: row.start_key})
    OPTIONAL MATCH (p:Person {recordId: row.start_key})
    WITH row, COALESCE(e, p) AS start
    WHERE start IS NOT NULL
    MATCH (id:Identifier {uid: row.end_key})
    MERGE (start)-[r:HAS_IDENTIFIER]->(id)
    SET r.ordinal = toInteger(row.ordinal),
        r.isPrimary = row.isPrimary = 'true'
} IN TRANSACTIONS OF 10000 ROWS;
"""

_HAS_ADDRESS_LOAD = """
LOAD CSV WITH HEADERS FROM 'file:///has_address.csv' AS row
CALL {
    WITH row
    OPTIONAL MATCH (e:Entity {recordId: row.start_key})
    OPTIONAL MATCH (p:Person {recordId: row.start_key})
    WITH row, COALESCE(e, p) AS start
    WHERE start IS NOT NULL
    MATCH (a:Address {uid: row.end_key})
    MERGE (start)-[r:HAS_ADDRESS {type: row.type, ordinal: toInteger(row.ordinal)}]->(a)
} IN TRANSACTIONS OF 10000 ROWS;
"""

_LOCATED_IN_LOAD = """
LOAD CSV WITH HEADERS FROM 'file:///located_in.csv' AS row
CALL {
    WITH row
    MATCH (a:Address {uid: row.start_key})
    MATCH (c:Country {code: row.end_key})
    MERGE (a)-[:LOCATED_IN]->(c)
} IN TRANSACTIONS OF 10000 ROWS;
"""

_REGISTERED_IN_LOAD = """
LOAD CSV WITH HEADERS FROM 'file:///registered_in.csv' AS row
CALL {
    WITH row
    MATCH (e:Entity {recordId: row.start_key})
    MATCH (c:Country {code: row.end_key})
    MERGE (e)-[r:REGISTERED_IN]->(c)
    SET r.jurisdictionName = row.jurisdictionName
} IN TRANSACTIONS OF 10000 ROWS;
"""

_BORN_IN_LOAD = """
LOAD CSV WITH HEADERS FROM 'file:///born_in.csv' AS row
CALL {
    WITH row
    MATCH (p:Person {recordId: row.start_key})
    MATCH (a:Address {uid: row.end_key})
    MERGE (p)-[:BORN_IN]->(a)
} IN TRANSACTIONS OF 10000 ROWS;
"""

_FULLTEXT_INDEX = """
CREATE FULLTEXT INDEX bods_names IF NOT EXISTS
FOR (n:Entity|Person) ON EACH [n.name];
"""

_SUMMARY_QUERY = """
MATCH (e:Entity) WITH count(e) AS entities
MATCH (p:Person) WITH entities, count(p) AS persons
OPTIONAL MATCH ()-[r:OWNS|CONTROLS|MANAGES|IS_PARTY_TO|HAS_OTHER_INTEREST]->()
WITH entities, persons, count(DISTINCT r.statementId) AS relationshipStatements
RETURN entities, persons, relationshipStatements;
"""
