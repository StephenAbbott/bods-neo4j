"""Tests against official BODS v0.4 example data files.

These files are from https://github.com/openownership/data-standard/tree/0.4.0/examples
and represent the canonical test cases for the BODS v0.4 schema.
"""

import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.csv_exporter import export_to_csv
from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.bods_to_neo4j.reader import count_statements, read_bods_file
from bods_neo4j.config import PublisherConfig
from bods_neo4j.neo4j_to_bods.mapper import (
    map_entity_node,
    map_person_node,
    map_relationship,
)

from ._graph_state import GraphState

EXAMPLES_DIR = Path(__file__).parent / "fixtures" / "official_examples"
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.json")) if EXAMPLES_DIR.exists() else []
PUBLISHER_CONFIG = PublisherConfig(publisher_name="Test Publisher", bods_version="0.4")


@pytest.fixture(params=EXAMPLE_FILES, ids=[f.stem for f in EXAMPLE_FILES])
def example_file(request):
    return request.param


def _primary(graph):
    return graph["nodes"][0] if graph["nodes"] else None


def _has_label(graph, label):
    return any(label in n["labels"] for n in graph["nodes"])


class TestReadOfficialExamples:
    def test_read_all_statements(self, example_file):
        statements = list(read_bods_file(example_file))
        assert len(statements) > 0, f"No statements found in {example_file.name}"

    def test_all_statements_have_required_fields(self, example_file):
        for statement in read_bods_file(example_file):
            assert "statementId" in statement
            assert "recordId" in statement
            assert "recordType" in statement
            assert statement["recordType"] in ("entity", "person", "relationship")
            assert "recordDetails" in statement

    def test_count_statements(self, example_file):
        counts = count_statements(example_file)
        statements = list(read_bods_file(example_file))
        assert counts["total"] == len(statements)
        assert counts["other"] == 0


class TestMapOfficialExamples:
    def test_all_statements_mappable(self, example_file):
        for statement in read_bods_file(example_file):
            graph = map_statement(statement)
            assert graph is not None, (
                f"Failed to map {statement.get('statementId')} in {example_file.name}"
            )

    def test_entity_statements_emit_entity_node(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "entity":
                continue
            graph = map_statement(statement)
            assert graph["statement_type"] == "entity"
            assert _has_label(graph, "Entity")
            assert _primary(graph)["properties"]["recordId"] == statement["recordId"]

    def test_person_statements_emit_person_node(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "person":
                continue
            graph = map_statement(statement)
            assert graph["statement_type"] == "person"
            assert _has_label(graph, "Person")

    def test_relationship_statements_emit_typed_edges(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            graph = map_statement(statement)
            assert graph["statement_type"] == "relationship"
            # At least one family edge (even synthetic when interests[] is
            # empty in the source).
            family_edges = [
                e for e in graph["edges"]
                if e["rel_type"] in {"OWNS", "CONTROLS", "MANAGES",
                                     "IS_PARTY_TO", "HAS_OTHER_INTEREST"}
            ]
            assert family_edges, (
                f"{example_file.name}: relationship statement "
                f"{statement.get('statementId')} produced no family-typed edge"
            )

    def test_relationship_endpoints_extracted(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            graph = map_statement(statement)
            subject = statement["recordDetails"].get("subject", "")
            ip = statement["recordDetails"].get("interestedParty", "")
            if isinstance(subject, str) and subject:
                assert graph["subject_record_id"] == subject
            if isinstance(ip, str) and ip:
                assert graph["interested_party_record_id"] == ip


class TestEntitySubtypeLabels:
    def test_trust_labels(self):
        for filename in ("fermcat.json", "nomination.json"):
            filepath = EXAMPLES_DIR / filename
            if not filepath.exists():
                continue
            for statement in read_bods_file(filepath):
                if statement["recordType"] != "entity":
                    continue
                details = statement["recordDetails"]
                entity_type = details.get("entityType", {})
                if entity_type.get("subtype") == "trust":
                    graph = map_statement(statement)
                    labels = set(_primary(graph)["labels"])
                    assert "Arrangement" in labels
                    assert "Trust" in labels

    def test_state_body_labels(self):
        filepath = EXAMPLES_DIR / "bods-package-fi-soe.json"
        if not filepath.exists():
            pytest.skip("SOE example not available")
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "entity":
                continue
            entity_type = statement["recordDetails"].get("entityType", {})
            if entity_type.get("type") == "stateBody":
                graph = map_statement(statement)
                assert "StateBody" in _primary(graph)["labels"]


class TestCsvExportOfficialExamples:
    def test_csv_export_all_examples(self, example_file):
        with tempfile.TemporaryDirectory() as tmpdir:
            counts = export_to_csv(example_file, tmpdir)
            total = (
                counts["entity_statements"]
                + counts["person_statements"]
                + counts["relationship_statements"]
            )
            assert total > 0, f"No records exported from {example_file.name}"
            assert counts["skipped"] == 0
            assert (Path(tmpdir) / "import.cypher").exists()
            assert (Path(tmpdir) / "import.sh").exists()

    def test_csv_counts_match_statements(self, example_file):
        """Every input statement is accounted for: exported + superseded
        + malformed sums to the input total. Per-type counts can be lower
        than the input because the pre-scan dedup filter drops versioned
        relationship statements (e.g. fermcat re-issues the same recordId
        multiple times) before they reach the CSVs."""
        statement_counts = count_statements(example_file)
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_counts = export_to_csv(example_file, tmpdir)
            total_input = (
                statement_counts["entity"]
                + statement_counts["person"]
                + statement_counts["relationship"]
            )
            total_exported = (
                csv_counts["entity_statements"]
                + csv_counts["person_statements"]
                + csv_counts["relationship_statements"]
            )
            assert (
                total_exported
                + csv_counts["skipped_superseded"]
                + csv_counts["skipped"]
            ) == total_input
            assert csv_counts["entity_statements"] <= statement_counts["entity"]
            assert csv_counts["person_statements"] <= statement_counts["person"]
            assert csv_counts["relationship_statements"] <= statement_counts["relationship"]


class TestRoundTripOfficialExamples:
    """BODS -> graph state -> BODS round-trip for every official example.

    Builds a fresh GraphState per statement so that records with multiple
    versions (same recordId, different statementIds — e.g. fermcat) all
    round-trip rather than the latest version overwriting the earlier ones.
    """

    def test_entity_round_trip(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "entity":
                continue
            state = GraphState.from_statements([statement])
            env = state.entity_envelope(statement["recordId"])
            out = map_entity_node(env, PUBLISHER_CONFIG)
            assert out["statementId"] == statement["statementId"]
            assert out["recordId"] == statement["recordId"]
            assert out["recordType"] == "entity"
            orig_type = statement["recordDetails"].get("entityType", {})
            rt_type = out["recordDetails"].get("entityType", {})
            assert rt_type.get("type") == orig_type.get("type")
            if orig_type.get("subtype"):
                assert rt_type.get("subtype") == orig_type.get("subtype")
            assert out["recordDetails"].get("name", "") == (
                statement["recordDetails"].get("name", "")
            )
            assert out["recordDetails"].get("identifiers", []) == (
                statement["recordDetails"].get("identifiers", [])
            )
            assert out["recordDetails"].get("addresses", []) == (
                statement["recordDetails"].get("addresses", [])
            )

    def test_person_round_trip(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "person":
                continue
            state = GraphState.from_statements([statement])
            env = state.person_envelope(statement["recordId"])
            out = map_person_node(env, PUBLISHER_CONFIG)
            assert out["statementId"] == statement["statementId"]
            assert out["recordId"] == statement["recordId"]
            assert out["recordType"] == "person"
            assert out["recordDetails"]["personType"] == (
                statement["recordDetails"]["personType"]
            )
            assert out["recordDetails"].get("names", []) == (
                statement["recordDetails"].get("names", [])
            )
            assert out["recordDetails"].get("nationalities", []) == (
                statement["recordDetails"].get("nationalities", [])
            )

    def test_relationship_round_trip(self, example_file):
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            state = GraphState.from_statements([statement])
            env = state.relationship_envelope(statement["statementId"])
            out = map_relationship(env, PUBLISHER_CONFIG)
            assert out["statementId"] == statement["statementId"]
            assert out["recordId"] == statement["recordId"]
            assert out["recordType"] == "relationship"
            orig_subject = statement["recordDetails"].get("subject", "")
            orig_ip = statement["recordDetails"].get("interestedParty", "")
            if isinstance(orig_subject, str) and orig_subject:
                assert out["recordDetails"]["subject"] == orig_subject
            if isinstance(orig_ip, str) and orig_ip:
                assert out["recordDetails"]["interestedParty"] == orig_ip
            assert out["recordDetails"].get("interests", []) == (
                statement["recordDetails"].get("interests", [])
            )


class TestSpecificExamples:
    def test_fermcat_complex_structure(self):
        filepath = EXAMPLES_DIR / "fermcat.json"
        if not filepath.exists():
            pytest.skip("fermcat.json not available")
        counts = count_statements(filepath)
        assert counts["entity"] > 0
        assert counts["person"] > 0
        assert counts["relationship"] > 0
        assert counts["total"] >= 10

    def test_joint_ownership(self):
        filepath = EXAMPLES_DIR / "joint-ownership.json"
        if not filepath.exists():
            pytest.skip("Joint ownership example not available")
        counts = count_statements(filepath)
        assert counts["relationship"] >= 2

    def test_soe_state_body(self):
        filepath = EXAMPLES_DIR / "bods-package-fi-soe.json"
        if not filepath.exists():
            pytest.skip("SOE example not available")
        found = False
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "entity":
                continue
            entity_type = statement["recordDetails"].get("entityType", {})
            if entity_type.get("type") in ("state", "stateBody"):
                graph = map_statement(statement)
                labels = set(_primary(graph)["labels"])
                if "State" in labels or "StateBody" in labels:
                    found = True
        assert found, "No state/stateBody entity found in SOE example"
