"""Tests for the dedup-on-ingest semantics: relationship statements with the
same `recordId` or named in `replacesStatements` evict their predecessors,
entity/person statements collapse via `replacesStatements`, and re-ingesting
the same statement is idempotent. Also asserts the new
`replacesStatements` passthrough property survives the reverse round-trip.

Runs in-process against ``GraphState.ingest_with_dedup`` (mirrors the
loader's pre-scan filter semantics) — no live Neo4j needed.

The loader and CSV exporter both implement dedup via a single-pass file
scan that filters superseded statements before they reach Neo4j / the CSV
files (see ``bods_neo4j.utils.dedup.scan_for_dedup`` / ``is_loser``);
those helpers are unit-tested directly in TestPreScanFilter below.
"""

import json
from pathlib import Path

from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.config import PublisherConfig
from bods_neo4j.neo4j_to_bods.mapper import (
    map_entity_node,
    map_person_node,
    map_relationship,
)
from bods_neo4j.utils.bods_schema import FAMILY_REL_TYPES
from bods_neo4j.utils.dedup import is_loser, scan_for_dedup

from ._graph_state import GraphState


def _entity_statement(statement_id, record_id, name, replaces=None):
    statement = {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "entity",
        "statementDate": "2024-01-01",
        "publicationDetails": {
            "publicationDate": "2024-01-01",
            "bodsVersion": "0.4",
            "publisher": {"name": "Test"},
        },
        "recordDetails": {
            "entityType": {"type": "registeredEntity"},
            "name": name,
            "isComponent": False,
        },
    }
    if replaces:
        statement["replacesStatements"] = list(replaces)
    return statement


def _person_statement(statement_id, record_id, full_name, replaces=None):
    statement = {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "person",
        "statementDate": "2024-01-01",
        "publicationDetails": {
            "publicationDate": "2024-01-01",
            "bodsVersion": "0.4",
            "publisher": {"name": "Test"},
        },
        "recordDetails": {
            "personType": "knownPerson",
            "isComponent": False,
            "names": [{"type": "individual", "fullName": full_name}],
        },
    }
    if replaces:
        statement["replacesStatements"] = list(replaces)
    return statement


def _relationship_statement(
    statement_id, record_id, subject, party, share_exact, replaces=None,
):
    statement = {
        "statementId": statement_id,
        "recordId": record_id,
        "recordType": "relationship",
        "statementDate": "2024-01-01",
        "publicationDetails": {
            "publicationDate": "2024-01-01",
            "bodsVersion": "0.4",
            "publisher": {"name": "Test"},
        },
        "recordDetails": {
            "subject": subject,
            "interestedParty": party,
            "isComponent": False,
            "interests": [
                {
                    "type": "shareholding",
                    "directOrIndirect": "direct",
                    "share": {"exact": share_exact},
                }
            ],
        },
    }
    if replaces:
        statement["replacesStatements"] = list(replaces)
    return statement


def _family_edges_for_record(state: GraphState, record_id: str) -> list:
    out = []
    for fam in FAMILY_REL_TYPES:
        for e in state.edges.get(fam, []):
            if e["properties"].get("recordId") == record_id:
                out.append(e)
    return out


def _family_edges_for_statement(state: GraphState, statement_id: str) -> list:
    out = []
    for fam in FAMILY_REL_TYPES:
        for e in state.edges.get(fam, []):
            if e["properties"].get("statementId") == statement_id:
                out.append(e)
    return out


class TestRelationshipDedup:
    def test_dedup_by_record_id_same_record(self):
        """Second relationship statement with the same `recordId` evicts
        the first one's family edges."""
        state = GraphState()
        # Two parties so the relationship endpoints resolve.
        state.ingest_with_dedup(_entity_statement("e1", "rec-co", "Co Ltd"))
        state.ingest_with_dedup(_person_statement("p1", "rec-person", "Alice"))

        state.ingest_with_dedup(_relationship_statement(
            "rel-stmt-old", "rec-rel-1", "rec-co", "rec-person", 0.5,
        ))
        assert len(_family_edges_for_statement(state, "rel-stmt-old")) == 1

        state.ingest_with_dedup(_relationship_statement(
            "rel-stmt-new", "rec-rel-1", "rec-co", "rec-person", 0.75,
        ))
        assert _family_edges_for_statement(state, "rel-stmt-old") == []
        new_edges = _family_edges_for_statement(state, "rel-stmt-new")
        assert len(new_edges) == 1
        assert new_edges[0]["properties"]["shareExact"] == 0.75

    def test_dedup_by_replaces_statements_different_record(self):
        """Statement B (different `recordId`) names statement A in
        `replacesStatements`; A's edges + sentinels should be evicted."""
        state = GraphState()
        state.ingest_with_dedup(_entity_statement("e1", "rec-co", "Co Ltd"))
        state.ingest_with_dedup(_person_statement("p1", "rec-person", "Alice"))

        state.ingest_with_dedup(_relationship_statement(
            "stmt-A", "rec-rel-A", "rec-co", "rec-person", 0.5,
        ))
        state.ingest_with_dedup(_relationship_statement(
            "stmt-B", "rec-rel-B", "rec-co", "rec-person", 0.9,
            replaces=["stmt-A"],
        ))

        assert _family_edges_for_statement(state, "stmt-A") == []
        assert len(_family_edges_for_statement(state, "stmt-B")) == 1

    def test_idempotent_re_ingest_same_statement(self):
        """Ingesting the same relationship statement twice keeps the
        family-edge count constant — the dedup-by-recordId pass evicts the
        first set before the second is written."""
        state = GraphState()
        state.ingest_with_dedup(_entity_statement("e1", "rec-co", "Co Ltd"))
        state.ingest_with_dedup(_person_statement("p1", "rec-person", "Alice"))

        stmt = _relationship_statement(
            "stmt-X", "rec-rel-X", "rec-co", "rec-person", 0.5,
        )
        state.ingest_with_dedup(stmt)
        state.ingest_with_dedup(stmt)

        assert len(_family_edges_for_record(state, "rec-rel-X")) == 1


class TestEntityCollapse:
    def test_entity_collapse_via_replaces_different_record_id(self):
        """Entity B with a new `recordId` and `replacesStatements: [A]`
        evicts Entity A entirely."""
        state = GraphState()
        state.ingest_with_dedup(_entity_statement("S1", "R1", "OldCo"))
        assert ("Entity", "R1") in state.nodes

        state.ingest_with_dedup(
            _entity_statement("S2", "R2", "NewCo", replaces=["S1"]),
        )
        assert ("Entity", "R1") not in state.nodes
        assert ("Entity", "R2") in state.nodes
        assert state.nodes[("Entity", "R2")]["replacesStatements"] == ["S1"]

    def test_person_collapse_via_replaces_different_record_id(self):
        """Same collapse-by-statementId for :Person nodes."""
        state = GraphState()
        state.ingest_with_dedup(_person_statement("PS1", "PR1", "Alice"))
        state.ingest_with_dedup(
            _person_statement("PS2", "PR2", "Bob", replaces=["PS1"]),
        )
        assert ("Person", "PR1") not in state.nodes
        assert ("Person", "PR2") in state.nodes


class TestReplacesStatementsRoundTrip:
    """The combined `replacesStatements` list survives the forward + reverse
    round-trip as a top-level scalar on the output BODS statement."""

    def _pub(self):
        return PublisherConfig(publisher_name="Test", bods_version="0.4")

    def test_entity_replaces_statements_round_trip(self):
        state = GraphState()
        state.ingest_with_dedup(_entity_statement("E0", "R0", "OldCo"))
        state.ingest_with_dedup(
            _entity_statement("E1", "R1", "NewCo", replaces=["E0"]),
        )

        env = state.entity_envelope("R1")
        out = map_entity_node(env, self._pub())
        assert out["replacesStatements"] == ["E0"]

    def test_person_replaces_statements_round_trip(self):
        state = GraphState()
        state.ingest_with_dedup(_person_statement("P0", "PR0", "Alice"))
        state.ingest_with_dedup(
            _person_statement("P1", "PR1", "Bob", replaces=["P0"]),
        )

        env = state.person_envelope("PR1")
        out = map_person_node(env, self._pub())
        assert out["replacesStatements"] == ["P0"]

    def test_relationship_replaces_statements_round_trip(self):
        state = GraphState()
        state.ingest_with_dedup(_entity_statement("e1", "rec-co", "Co Ltd"))
        state.ingest_with_dedup(_person_statement("p1", "rec-person", "Alice"))
        state.ingest_with_dedup(
            _relationship_statement("rs-old", "rec-rel", "rec-co", "rec-person", 0.5),
        )
        state.ingest_with_dedup(
            _relationship_statement(
                "rs-new", "rec-rel", "rec-co", "rec-person", 0.9,
                replaces=["rs-old"],
            ),
        )

        env = state.relationship_envelope("rs-new")
        out = map_relationship(env, self._pub())
        assert out["replacesStatements"] == ["rs-old"]


class TestMapperReturnsReplacesMetadata:
    """The forward mapper surfaces `replaces_statements` / `record_id` on the
    returned graph dict so the loader can use them as delete-triggers.
    """

    def test_entity_returns_combined_replaces(self):
        stmt = _entity_statement("E1", "R1", "Co", replaces=["A", "B"])
        graph = map_statement(stmt)
        assert graph["record_id"] == "R1"
        assert graph["replaces_statements"] == ["A", "B"]

    def test_relationship_returns_combined_replaces(self):
        stmt = _relationship_statement(
            "rs", "rr", "rec-co", "rec-person", 0.5, replaces=["X"],
        )
        graph = map_statement(stmt)
        assert graph["record_id"] == "rr"
        assert graph["replaces_statements"] == ["X"]

    def test_entity_merges_top_level_and_details(self):
        stmt = _entity_statement("E1", "R1", "Co")
        stmt["replacesStatements"] = ["top-A"]
        stmt["recordDetails"]["replacesStatements"] = ["details-B"]
        graph = map_statement(stmt)
        assert graph["replaces_statements"] == ["top-A", "details-B"]


class TestPreScanFilter:
    """Unit-test ``scan_for_dedup`` + ``is_loser`` — the file-walk filter
    that keeps superseded statements out of Neo4j / CSV entirely."""

    def _write_jsonl(self, tmp_path: Path, statements: list[dict]) -> Path:
        path = tmp_path / "input.jsonl"
        path.write_text("\n".join(json.dumps(s) for s in statements) + "\n")
        return path

    def test_relationship_latest_by_statement_date_wins(self, tmp_path):
        older = _relationship_statement("rs-old", "rec-rel", "rec-co", "rec-p", 0.5)
        older["statementDate"] = "2023-01-01"
        newer = _relationship_statement("rs-new", "rec-rel", "rec-co", "rec-p", 0.9)
        newer["statementDate"] = "2024-01-01"

        path = self._write_jsonl(tmp_path, [older, newer])
        scan = scan_for_dedup(path)

        assert scan.latest_statement_per_record == {"rec-rel": "rs-new"}
        assert is_loser(older, scan) is True
        assert is_loser(newer, scan) is False

    def test_relationship_lexicographic_tiebreak_when_dates_equal(self, tmp_path):
        a = _relationship_statement("aaa", "rec-rel", "rec-co", "rec-p", 0.5)
        b = _relationship_statement("zzz", "rec-rel", "rec-co", "rec-p", 0.9)
        # Same statementDate by construction; tiebreak goes to lexicographic max.

        path = self._write_jsonl(tmp_path, [a, b])
        scan = scan_for_dedup(path)

        assert scan.latest_statement_per_record == {"rec-rel": "zzz"}
        assert is_loser(a, scan) is True
        assert is_loser(b, scan) is False

    def test_replaces_statements_marks_target_as_loser(self, tmp_path):
        old = _entity_statement("E-old", "R1", "OldCo")
        new = _entity_statement("E-new", "R2", "NewCo", replaces=["E-old"])

        path = self._write_jsonl(tmp_path, [old, new])
        scan = scan_for_dedup(path)

        assert "E-old" in scan.superseded_statement_ids
        assert is_loser(old, scan) is True
        assert is_loser(new, scan) is False

    def test_replaces_statements_inside_record_details(self, tmp_path):
        old = _person_statement("P-old", "PR1", "Alice")
        new = _person_statement("P-new", "PR2", "Bob")
        new["recordDetails"]["replacesStatements"] = ["P-old"]

        path = self._write_jsonl(tmp_path, [old, new])
        scan = scan_for_dedup(path)

        assert "P-old" in scan.superseded_statement_ids
        assert is_loser(old, scan) is True

    def test_statement_without_record_or_id_is_not_a_loser(self, tmp_path):
        # Malformed statements (missing recordId / statementId) should not
        # be marked as losers by the filter — they'll be skipped by the
        # forward mapper itself or counted as malformed.
        ok = _entity_statement("E1", "R1", "Co")
        path = self._write_jsonl(tmp_path, [ok])
        scan = scan_for_dedup(path)

        assert is_loser({}, scan) is False
        assert is_loser({"recordType": "relationship"}, scan) is False

    def test_relationship_in_superseded_set_is_a_loser(self, tmp_path):
        # A relationship statement explicitly retired via another's
        # replacesStatements should be filtered even if it happens to be
        # the latest for its recordId.
        rel = _relationship_statement("rel-1", "rec-rel-1", "rec-co", "rec-p", 0.5)
        retiring = _entity_statement("e-retire", "rec-r", "Co", replaces=["rel-1"])

        path = self._write_jsonl(tmp_path, [rel, retiring])
        scan = scan_for_dedup(path)

        assert is_loser(rel, scan) is True
