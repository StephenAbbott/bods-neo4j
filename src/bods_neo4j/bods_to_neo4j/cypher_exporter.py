"""Export BODS data to one self-contained, APOC-free Cypher script.

The CSV route (``csv_exporter``) needs a Neo4j server with APOC Core and the
default ``legacy_quote_escaping`` setting: list properties are JSON in a CSV
cell and are parsed back with ``apoc.convert.fromJsonList``. Engines without
APOC — Neo4j without the plugin, Memgraph without MAGE, embedded engines such
as Relate — cannot load those files.

This exporter writes the same graph as plain Cypher:

* every value is a **native literal** (strings, numbers, booleans, lists,
  ``date('YYYY-MM-DD')`` for the full dates in ``DATE_PROPS``), so nothing is
  cast or parsed at load time and no procedure is called;
* rows go in ``UNWIND [ {...}, ... ] AS row`` batches of ``batch_size``
  (default 200: 1,000-row batches of the Estonia file ran a default 1 GB
  Neo4j heap out of memory), one autocommit statement per batch — no
  ``CALL { } IN TRANSACTIONS``, which is Neo4j-only;
* the endpoints of every edge are resolved in Python, so each edge batch
  matches on one label and one key (``MATCH (a:Person {recordId: row.s})``)
  instead of the ``OPTIONAL MATCH`` × 3 + ``COALESCE`` the CSV route needs.
  An edge whose endpoint is not in the file is left out, as both other
  routes leave it out;
* no file access: the script carries its data, so ``LOAD CSV`` permissions
  and import directories do not apply.

Indexes are the one thing Cypher dialects spell differently, so the schema
block is chosen with ``dialect``: ``neo4j`` (Neo4j 5
``CREATE CONSTRAINT … IF NOT EXISTS``), ``memgraph`` (``CREATE INDEX ON``)
or ``none``. Without an index every ``MERGE`` and edge ``MATCH`` is a label
scan, which is fine for a FullCheck network and slow for a national dataset.
The full-text index the CSV route creates is left out for the same reason.

The graph is property-for-property the graph ``to-neo4j`` (the driver route)
builds; ``tests/test_cypher_exporter.py`` pins that.
"""

from __future__ import annotations

import datetime as _dt
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import IO, Iterable, Union

from ..utils.bods_schema import FAMILY_REL_TYPES
from ..utils.property_types import DATE_PROPS, to_neo4j_value
from .csv_exporter import (
    _ENTITY_SUBTYPE_FIXUP,
    _NODE_LABELS,
    _STRUCTURAL_REL_TYPES,
    collect_graph,
)

logger = logging.getLogger(__name__)

DIALECTS = ("neo4j", "memgraph", "none")

#: (label, key property) for every node label the mapper emits.
_NODE_KEYS = {
    "Country": "code",
    "Identifier": "uid",
    "Address": "uid",
    "Entity": "recordId",
    "Person": "recordId",
    "UnspecifiedParty": "uid",
}

#: Structural edges are MERGEd (one per pair); HAS_ADDRESS per pair, type and
#: ordinal, so one address can play several roles. Family edges are CREATEd:
#: each (statementId, interestIndex) is its own edge, as in the other routes.
_MERGE_KEYS = {"HAS_ADDRESS": ("type", "ordinal")}


def export_to_cypher(
    bods_file: Union[str, Path],
    output_path: Union[str, Path] = "./bods.cypher",
    identifiers_property: bool = False,
    dialect: str = "neo4j",
    batch_size: int = 200,
    stamp_latest_versions: bool = True,
) -> dict:
    """Write the graph for ``bods_file`` as one Cypher script.

    ``stamp_latest_versions`` sets ``isLatestVersion: true`` on every
    interest edge, as the driver route does (superseded statements are
    already filtered out, so every edge written is the latest).

    Returns the statement counts plus ``nodes`` / ``edges`` per label / type
    and ``edges_dropped`` (edges whose endpoint is not in the file).
    """
    if dialect not in DIALECTS:
        raise ValueError(f"dialect must be one of {DIALECTS}, not {dialect!r}")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    counts, nodes_by_label, edges_by_type = collect_graph(
        bods_file, identifiers_property=identifiers_property,
    )
    if stamp_latest_versions:
        for rel_type in FAMILY_REL_TYPES:
            for edge in edges_by_type.get(rel_type) or []:
                edge["properties"]["isLatestVersion"] = True
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="\n") as out:
        node_totals, edge_totals, dropped = write_cypher(
            out, nodes_by_label, edges_by_type, dialect=dialect, batch_size=batch_size,
        )
    counts["nodes"] = node_totals
    counts["edges"] = edge_totals
    counts["edges_dropped"] = dropped
    logger.info("Wrote %s (%s nodes, %s edges, %s edges without an endpoint)",
                output_path, sum(node_totals.values()), sum(edge_totals.values()), dropped)
    return counts


def write_cypher(
    out: IO[str],
    nodes_by_label: dict[str, dict],
    edges_by_type: dict[str, list],
    dialect: str = "neo4j",
    batch_size: int = 200,
) -> tuple[dict, dict, int]:
    """Write the script for an already collected graph to ``out``."""
    out.write(_HEADER)
    out.write(_schema_block(dialect))

    node_totals: dict[str, int] = {}
    # (label, key) of every node written, so each edge endpoint is resolved here.
    written: set[tuple[str, str]] = set()
    for label in _NODE_LABELS:
        bucket = nodes_by_label.get(label) or {}
        if not bucket:
            continue
        key_prop = _NODE_KEYS[label]
        out.write(f"\n// :{label} ({len(bucket):,})\n")
        rows = [
            {"k": key, "p": _clean_props(spec["properties"], drop=(key_prop,))}
            for key, spec in bucket.items()
        ]
        written.update((label, key) for key in bucket)
        _write_batches(
            out, rows, batch_size,
            f"MERGE (n:{label} {{{key_prop}: row.k}}) SET n += row.p",
        )
        node_totals[label] = len(rows)
        if label == "Entity":
            out.write("\n// Entity subtype labels\n")
            out.write(_ENTITY_SUBTYPE_FIXUP.strip() + "\n")

    edge_totals: dict[str, int] = {}
    dropped = 0
    for rel_type in _STRUCTURAL_REL_TYPES + FAMILY_REL_TYPES:
        edges = edges_by_type.get(rel_type) or []
        if not edges:
            continue
        groups: dict[tuple, list] = defaultdict(list)
        for e in edges:
            start = _endpoint(e, "start", written)
            end = _endpoint(e, "end", written)
            if start is None or end is None:
                dropped += 1
                continue
            groups[(start, end)].append(
                {"s": e["start_key_value"], "t": e["end_key_value"],
                 "p": _clean_props(e["properties"])}
            )
        if rel_type not in FAMILY_REL_TYPES:
            groups = {pair: _fold_merged(rows, _MERGE_KEYS.get(rel_type, ()))
                      for pair, rows in groups.items()}
        total = 0
        for ((s_label, s_key), (t_label, t_key)), rows in groups.items():
            out.write(f"\n// ({s_label})-[:{rel_type}]->({t_label}) ({len(rows):,})\n")
            match = (f"MATCH (a:{s_label} {{{s_key}: row.s}}) "
                     f"MATCH (b:{t_label} {{{t_key}: row.t}}) ")
            if rel_type in FAMILY_REL_TYPES:
                body = f"CREATE (a)-[r:{rel_type}]->(b) SET r += row.p"
            elif rel_type in _MERGE_KEYS:
                keys = ", ".join(f"{k}: row.p.{k}" for k in _MERGE_KEYS[rel_type])
                body = f"MERGE (a)-[r:{rel_type} {{{keys}}}]->(b) SET r += row.p"
            else:
                body = f"MERGE (a)-[r:{rel_type}]->(b) SET r += row.p"
            _write_batches(out, rows, batch_size, match + body)
            total += len(rows)
        if total:
            edge_totals[rel_type] = total

    out.write(_SUMMARY_QUERY)
    return node_totals, edge_totals, dropped


def _fold_merged(rows: list[dict], keys: tuple[str, ...]) -> list[dict]:
    """Fold rows that MERGE onto one edge into one row.

    The database would fold them anyway (each later ``SET r += row.p``
    overwrites the earlier keys); folding here makes the batch, and the
    count in the header, match the edges the script creates.
    """
    folded: dict[tuple, dict] = {}
    for row in rows:
        k = (row["s"], row["t"], *(repr(row["p"].get(x)) for x in keys))
        if k in folded:
            folded[k]["p"] = {**folded[k]["p"], **row["p"]}
        else:
            folded[k] = {"s": row["s"], "t": row["t"], "p": dict(row["p"])}
    return list(folded.values())


def _endpoint(edge: dict, side: str, written: set) -> tuple[str, str] | None:
    """``(label, key property)`` of one end of an edge, or None if absent.

    The mapper names a party endpoint ``Node`` whatever it is (a
    relationship statement does not say whether its party is a person), so
    a recordId is looked up among the Entity and Person nodes written.
    """
    key_prop = edge[f"{side}_key_property"]
    key = edge[f"{side}_key_value"]
    named = edge[f"{side}_label"]
    # The mapper names a party endpoint "Node" (Entity or Person).
    candidates = ("Entity", "Person") if named in ("Node", "Entity", "Person") else (named,)
    for label in candidates:
        if (label, key) in written:
            return label, key_prop
    return None


def _write_batches(out: IO[str], rows: list, batch_size: int, body: str) -> None:
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        out.write("UNWIND [\n")
        out.write(",\n".join("  " + literal(r) for r in batch))
        out.write(f"\n] AS row\n{body};\n")


def _clean_props(props: dict, drop: Iterable[str] = ()) -> dict:
    """Properties as the driver route stores them: typed dates, no nulls."""
    dropped = set(drop)
    out = {}
    for k, v in props.items():
        if k in dropped or v is None:
            continue
        if isinstance(v, float) and not math.isfinite(v):
            continue
        out[k] = to_neo4j_value(k, v) if k in DATE_PROPS else v
    return out


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------

_ESCAPES = {"\\": "\\\\", "'": "\\'", "\n": "\\n", "\r": "\\r", "\t": "\\t"}


def string_literal(value: str) -> str:
    """A single-quoted Cypher string literal that every engine reads back."""
    parts = []
    for ch in value:
        if ch in _ESCAPES:
            parts.append(_ESCAPES[ch])
        elif ord(ch) < 0x20 or 0x7F <= ord(ch) < 0xA0 or ch in "  ":
            parts.append(f"\\u{ord(ch):04x}")
        else:
            parts.append(ch)
    return "'" + "".join(parts) + "'"


def _key(name: str) -> str:
    if name.isidentifier() and name.isascii():
        return name
    return "`" + name.replace("`", "``") + "`"


def literal(value) -> str:
    """Render a Python value as a Cypher literal."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return "null"
        text = repr(value)
        return text if any(c in text for c in ".eE") else text + ".0"
    if isinstance(value, str):
        return string_literal(value)
    if isinstance(value, _dt.datetime):
        return f"datetime({string_literal(value.isoformat())})"
    if isinstance(value, _dt.date):
        return f"date({string_literal(value.isoformat())})"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(literal(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_key(k)}: {literal(v)}" for k, v in value.items()) + "}"
    raise TypeError(f"no Cypher literal for {type(value).__name__}")


# ---------------------------------------------------------------------------
# Script text
# ---------------------------------------------------------------------------

_HEADER = """// BODS graph — portable Cypher import, generated by bods-neo4j to-cypher.
// Self-contained: no CSV files, no APOC, no LOAD CSV, no CALL IN TRANSACTIONS.
// Every value is a native literal; one UNWIND batch per statement.
// Run it with cypher-shell (Neo4j), mgconsole (Memgraph) or any client that
// executes a ';'-separated script.
"""


def _schema_block(dialect: str) -> str:
    if dialect == "none":
        return ("\n// No indexes (dialect 'none'): MERGE and edge MATCH scan each "
                "label.\n// Create indexes on the keys below first for a large "
                "file.\n")
    lines = [f"\n// Indexes on the node keys ({dialect})"]
    for label, key in _NODE_KEYS.items():
        if dialect == "neo4j":
            name = f"bods_{label.lower()}_{key.lower()}"
            lines.append(f"CREATE CONSTRAINT {name} IF NOT EXISTS "
                         f"FOR (n:{label}) REQUIRE n.{key} IS UNIQUE;")
        else:  # memgraph
            lines.append(f"CREATE INDEX ON :{label}({key});")
    return "\n".join(lines) + "\n"


_SUMMARY_QUERY = """
// Summary
MATCH (e:Entity) WITH count(e) AS entities
MATCH (p:Person) WITH entities, count(p) AS persons
OPTIONAL MATCH ()-[r:OWNS|CONTROLS|MANAGES|IS_PARTY_TO|HAS_OTHER_INTEREST]->()
WITH entities, persons, count(DISTINCT r.statementId) AS relationshipStatements
RETURN entities, persons, relationshipStatements;
"""
