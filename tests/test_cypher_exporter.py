"""Tests for the portable (APOC-free) Cypher exporter."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.cypher_exporter import (
    export_to_cypher,
    literal,
    string_literal,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"
OFFICIAL = sorted((FIXTURES_DIR / "official_examples").glob("*.json"))


def _script(tmp_path, source=SAMPLE_FILE, **kw) -> str:
    out = tmp_path / "bods.cypher"
    export_to_cypher(source, out, **kw)
    return out.read_text(encoding="utf-8")


def _statements(script: str) -> list[str]:
    """Split a generated script into statements.

    A raw newline never occurs inside a generated string literal (it is
    written ``\\n``), so ``;`` at the end of a line ends a statement.
    """
    body = "\n".join(line for line in script.splitlines() if not line.startswith("//"))
    return [s.strip() for s in re.split(r";\n", body + "\n") if s.strip()]


class TestLiterals:
    @pytest.mark.parametrize("raw,expected", [
        ("plain", "'plain'"),
        ("O'Brien", "'O\\'Brien'"),
        ("C:\\path", "'C:\\\\path'"),
        ('say "hi"', "'say \"hi\"'"),
        ("two\nlines\r\ttab", "'two\\nlines\\r\\ttab'"),
        ("bell\x07", "'bell\\u0007'"),
        ("Zürich 東京 🏦", "'Zürich 東京 🏦'"),
    ])
    def test_string_literal(self, raw, expected):
        assert string_literal(raw) == expected

    def test_scalars_lists_maps_and_dates(self):
        assert literal(None) == "null"
        assert literal(True) == "true"
        assert literal(3) == "3"
        assert literal(25.0) == "25.0"
        assert literal(1e-7) == "1e-07"
        assert literal(float("nan")) == "null"
        assert literal(dt.date(2020, 1, 2)) == "date('2020-01-02')"
        assert literal(["a", 1, False]) == "['a', 1, false]"
        assert literal({"a": 1, "bad key": "x"}) == "{a: 1, `bad key`: 'x'}"

    def test_unknown_type_raises(self):
        with pytest.raises(TypeError):
            literal(object())


class TestScript:
    def test_no_apoc_no_load_csv_no_in_transactions(self, tmp_path):
        script = "\n".join(_statements(_script(tmp_path)))
        assert "apoc." not in script
        assert "LOAD CSV" not in script
        assert "IN TRANSACTIONS" not in script
        assert "file:///" not in script

    def test_values_are_typed_literals(self, tmp_path):
        script = _script(tmp_path)
        assert "statementDate: date('2024-01-15')" in script
        assert "shareMinimum: 50.0" in script
        assert "beneficialOwnershipOrControl: true" in script
        assert "isLatestVersion: true" in script

    def test_party_endpoints_are_resolved_to_their_label(self, tmp_path):
        script = _script(tmp_path)
        assert ("MATCH (a:Person {recordId: row.s}) MATCH (b:Entity {recordId: row.t}) "
                "CREATE (a)-[r:OWNS]->(b)") in script
        assert ("MATCH (a:Entity {recordId: row.s}) MATCH (b:Entity {recordId: row.t}) "
                "CREATE (a)-[r:OWNS]->(b)") in script
        assert "COALESCE" not in script

    def test_counts(self, tmp_path):
        counts = export_to_cypher(SAMPLE_FILE, tmp_path / "x.cypher")
        assert counts["nodes"]["Entity"] == 3
        assert counts["nodes"]["Person"] == 2
        assert counts["edges"]["OWNS"] == 3
        assert counts["edges_dropped"] == 0

    def test_an_edge_to_a_party_not_in_the_file_is_left_out(self, tmp_path):
        statements = json.loads(SAMPLE_FILE.read_text())
        kept = [s for s in statements if s.get("recordId") != "rec-person-alice"]
        src = tmp_path / "partial.json"
        src.write_text(json.dumps(kept))
        counts = export_to_cypher(src, tmp_path / "x.cypher")
        assert counts["edges"]["OWNS"] == 2
        assert counts["edges_dropped"] == 1

    def test_batches(self, tmp_path):
        script = _script(tmp_path, batch_size=1)
        # 3 entities → 3 Entity MERGE statements of one row each.
        assert script.count("MERGE (n:Entity {recordId: row.k})") == 3

    @pytest.mark.parametrize("dialect,needle", [
        ("neo4j", "CREATE CONSTRAINT bods_entity_recordid IF NOT EXISTS "
                  "FOR (n:Entity) REQUIRE n.recordId IS UNIQUE;"),
        ("memgraph", "CREATE INDEX ON :Entity(recordId);"),
    ])
    def test_dialects(self, tmp_path, dialect, needle):
        assert needle in _script(tmp_path, dialect=dialect)

    def test_no_schema_dialect(self, tmp_path):
        script = _script(tmp_path, dialect="none")
        assert "CREATE CONSTRAINT" not in script and "CREATE INDEX" not in script

    def test_bad_dialect(self, tmp_path):
        with pytest.raises(ValueError):
            export_to_cypher(SAMPLE_FILE, tmp_path / "x.cypher", dialect="sql")

    def test_identifiers_property(self, tmp_path):
        script = _script(tmp_path, identifiers_property=True)
        assert "identifiers: ['gb-coh:00112233']" in script

    @pytest.mark.parametrize("path", OFFICIAL, ids=lambda p: p.name)
    def test_every_official_example_exports(self, tmp_path, path):
        script = _script(tmp_path, source=path)
        assert _statements(script)


# ---------------------------------------------------------------------------
# Live: the script builds the same graph as the driver route.
# Set BODS_NEO4J_TEST_URI (e.g. bolt://localhost:7687, an EMPTY database —
# it is cleared) to run. Works on Neo4j 5 without APOC.
# ---------------------------------------------------------------------------

LIVE_URI = os.environ.get("BODS_NEO4J_TEST_URI")


def _snapshot(driver):
    def plain(v):
        if hasattr(v, "iso_format"):
            return ["date", v.iso_format()]
        if isinstance(v, list):
            return [plain(x) for x in v]
        if isinstance(v, float):
            return ["float", v]
        return v

    with driver.session() as s:
        key = {}
        nodes = []
        for r in s.run("MATCH (n) RETURN elementId(n) AS id, labels(n) AS l, "
                       "properties(n) AS p"):
            p = {k: plain(v) for k, v in r["p"].items()}
            key[r["id"]] = p.get("recordId") or p.get("uid") or p.get("code")
            nodes.append(json.dumps([sorted(r["l"]), p], sort_keys=True))
        rels = [
            json.dumps([key[r["a"]], r["t"], key[r["b"]],
                        {k: plain(v) for k, v in r["p"].items()}], sort_keys=True)
            for r in s.run("MATCH (a)-[r]->(b) RETURN elementId(a) AS a, type(r) AS t, "
                           "properties(r) AS p, elementId(b) AS b")
        ]
    return sorted(nodes), sorted(rels)


@pytest.mark.skipif(not LIVE_URI, reason="set BODS_NEO4J_TEST_URI to run against Neo4j")
@pytest.mark.parametrize("path", [SAMPLE_FILE, *OFFICIAL], ids=lambda p: p.name)
def test_script_builds_the_driver_routes_graph(tmp_path, path):
    from neo4j import GraphDatabase

    from bods_neo4j.bods_to_neo4j.driver_loader import load_bods_to_neo4j
    from bods_neo4j.config import ExportConfig, Neo4jConfig

    user = os.environ.get("BODS_NEO4J_TEST_USER", "neo4j")
    password = os.environ.get("BODS_NEO4J_TEST_PASSWORD", "")
    auth = (user, password) if password else None
    driver = GraphDatabase.driver(LIVE_URI, auth=auth)
    try:
        def clear():
            with driver.session() as s:
                s.run("MATCH (n) DETACH DELETE n").consume()

        clear()
        load_bods_to_neo4j(path, Neo4jConfig(uri=LIVE_URI, username=user,
                                              password=password), ExportConfig())
        expected = _snapshot(driver)
        clear()
        with driver.session() as s:
            for statement in _statements(_script(tmp_path, source=path)):
                s.run(statement).consume()
        assert _snapshot(driver) == expected
    finally:
        driver.close()
