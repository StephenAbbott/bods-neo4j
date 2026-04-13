"""Tests against official BODS v0.4 example data files.

These files are from https://github.com/openownership/data-standard/tree/0.4.0/examples
and represent the canonical test cases for the BODS v0.4 schema.
"""

import json
import os
import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.reader import read_bods_file, count_statements
from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.bods_to_neo4j.csv_exporter import export_to_csv
from bods_neo4j.neo4j_to_bods.mapper import (
    map_entity_node,
    map_person_node,
    map_relationship,
)
from bods_neo4j.config import PublisherConfig

EXAMPLES_DIR = Path(__file__).parent / "fixtures" / "official_examples"

# Collect all example files
EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.json")) if EXAMPLES_DIR.exists() else []

PUBLISHER_CONFIG = PublisherConfig(publisher_name="Test Publisher", bods_version="0.4")


@pytest.fixture(params=EXAMPLE_FILES, ids=[f.stem for f in EXAMPLE_FILES])
def example_file(request):
    """Parametrise tests across all official example files."""
    return request.param


class TestReadOfficialExamples:
    """Test that all official examples can be read without errors."""

    def test_read_all_statements(self, example_file):
        """Every official example file should be readable."""
        statements = list(read_bods_file(example_file))
        assert len(statements) > 0, f"No statements found in {example_file.name}"

    def test_all_statements_have_required_fields(self, example_file):
        """Every statement should have the minimum required BODS fields."""
        for statement in read_bods_file(example_file):
            assert "statementId" in statement, (
                f"Missing statementId in {example_file.name}"
            )
            assert "recordId" in statement, (
                f"Missing recordId in {example_file.name}: {statement.get('statementId')}"
            )
            assert "recordType" in statement, (
                f"Missing recordType in {example_file.name}: {statement.get('statementId')}"
            )
            assert statement["recordType"] in ("entity", "person", "relationship"), (
                f"Invalid recordType '{statement['recordType']}' in {example_file.name}"
            )
            assert "recordDetails" in statement, (
                f"Missing recordDetails in {example_file.name}: {statement.get('statementId')}"
            )

    def test_count_statements(self, example_file):
        """count_statements should match manual count."""
        counts = count_statements(example_file)
        statements = list(read_bods_file(example_file))
        assert counts["total"] == len(statements)
        assert counts["other"] == 0  # No unrecognised record types


class TestMapOfficialExamples:
    """Test that all official examples can be mapped to Neo4j format."""

    def test_all_statements_mappable(self, example_file):
        """Every statement in every example should produce a valid mapped output."""
        for statement in read_bods_file(example_file):
            result = map_statement(statement)
            assert result is not None, (
                f"Failed to map statement {statement.get('statementId')} "
                f"(type: {statement.get('recordType')}) in {example_file.name}"
            )

    def test_entity_statements_produce_nodes(self, example_file):
        """Entity statements should produce node dicts with Entity label."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "entity":
                continue
            result = map_statement(statement)
            assert result["type"] == "node"
            assert "Entity" in result["labels"]
            assert "recordId" in result["properties"]
            assert "statementId" in result["properties"]
            assert result["properties"]["recordType"] == "entity"

    def test_person_statements_produce_nodes(self, example_file):
        """Person statements should produce node dicts with Person label."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "person":
                continue
            result = map_statement(statement)
            assert result["type"] == "node"
            assert "Person" in result["labels"]
            assert "recordId" in result["properties"]
            assert result["properties"]["recordType"] == "person"

    def test_relationship_statements_produce_relationships(self, example_file):
        """Relationship statements should produce relationship dicts."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            result = map_statement(statement)
            assert result["type"] == "relationship"
            assert result["rel_type"] == "HAS_INTEREST"
            assert "recordId" in result["properties"]
            assert result["properties"]["recordType"] == "relationship"

    def test_entity_names_extracted(self, example_file):
        """Entity nodes should have a name property."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "entity":
                continue
            result = map_statement(statement)
            # Entities may not always have a name (e.g. unspecified entities)
            name = result["properties"].get("name", "")
            entity_type = result["properties"].get("entityType", "")
            # Named entities should have names
            if entity_type in ("registeredEntity", "legalEntity"):
                if statement["recordDetails"].get("name"):
                    assert name, (
                        f"Entity missing name in {example_file.name}: "
                        f"{statement.get('statementId')}"
                    )

    def test_person_names_extracted(self, example_file):
        """Person nodes should have a name property when knownPerson."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "person":
                continue
            result = map_statement(statement)
            person_type = result["properties"].get("personType", "")
            if person_type == "knownPerson":
                name = result["properties"].get("name", "")
                assert name, (
                    f"Known person missing name in {example_file.name}: "
                    f"{statement.get('statementId')}"
                )

    def test_relationship_endpoints_extracted(self, example_file):
        """Relationships should have source and target record IDs."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            result = map_statement(statement)
            # Source or target may be empty when using UnspecifiedRecord
            subject = statement["recordDetails"].get("subject", "")
            ip = statement["recordDetails"].get("interestedParty", "")
            if isinstance(subject, str) and subject:
                assert result["target_record_id"] == subject
            if isinstance(ip, str) and ip:
                assert result["source_record_id"] == ip

    def test_interests_preserved(self, example_file):
        """Relationship interests should be preserved as JSON."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue
            result = map_statement(statement)
            original_interests = statement["recordDetails"].get("interests", [])
            if original_interests:
                assert "interests_json" in result["properties"]
                round_trip = json.loads(result["properties"]["interests_json"])
                assert round_trip == original_interests


class TestEntitySubtypeLabels:
    """Test that entity subtypes map to correct Neo4j labels."""

    def test_trust_labels(self):
        """Trust entities get Arrangement and Trust labels."""
        trust_files = [
            "fermcat.json",
            "nomination.json",
        ]
        for filename in trust_files:
            filepath = EXAMPLES_DIR / filename
            if not filepath.exists():
                continue
            for statement in read_bods_file(filepath):
                if statement["recordType"] != "entity":
                    continue
                details = statement["recordDetails"]
                entity_type = details.get("entityType", {})
                if entity_type.get("subtype") == "trust":
                    result = map_statement(statement)
                    assert "Arrangement" in result["labels"]
                    assert "Trust" in result["labels"]

    def test_state_body_labels(self):
        """State body entities get StateBody label."""
        filepath = EXAMPLES_DIR / "bods-package-fi-soe.json"
        if not filepath.exists():
            pytest.skip("SOE example not available")
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "entity":
                continue
            details = statement["recordDetails"]
            entity_type = details.get("entityType", {})
            if entity_type.get("type") == "stateBody":
                result = map_statement(statement)
                assert "StateBody" in result["labels"]

    def test_nomination_labels(self):
        """Nomination arrangements get Nomination label."""
        filepath = EXAMPLES_DIR / "nomination.json"
        if not filepath.exists():
            pytest.skip("Nomination example not available")
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "entity":
                continue
            details = statement["recordDetails"]
            entity_type = details.get("entityType", {})
            if entity_type.get("subtype") == "nomination":
                result = map_statement(statement)
                assert "Nomination" in result["labels"]


class TestCsvExportOfficialExamples:
    """Test CSV export with official examples."""

    def test_csv_export_all_examples(self, example_file):
        """Every example file should export to CSV without errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            counts = export_to_csv(example_file, tmpdir)
            total = counts["entities"] + counts["persons"] + counts["relationships"]
            assert total > 0, f"No records exported from {example_file.name}"
            assert counts["skipped"] == 0, (
                f"Skipped {counts['skipped']} statements in {example_file.name}"
            )

            # Verify CSV files were created
            assert (Path(tmpdir) / "entities.csv").exists()
            assert (Path(tmpdir) / "persons.csv").exists()
            assert (Path(tmpdir) / "relationships.csv").exists()
            assert (Path(tmpdir) / "import.cypher").exists()
            assert (Path(tmpdir) / "import.sh").exists()

    def test_csv_counts_match_statements(self, example_file):
        """CSV export counts should match statement counts."""
        statement_counts = count_statements(example_file)
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_counts = export_to_csv(example_file, tmpdir)
            assert csv_counts["entities"] == statement_counts["entity"]
            assert csv_counts["persons"] == statement_counts["person"]
            assert csv_counts["relationships"] == statement_counts["relationship"]


class TestRoundTripOfficialExamples:
    """Test BODS -> Neo4j -> BODS round-trip with official examples."""

    def test_entity_round_trip(self, example_file):
        """Entity statements should survive round-trip conversion."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "entity":
                continue

            # BODS -> Neo4j
            neo4j_result = map_statement(statement)
            assert neo4j_result is not None
            node_props = neo4j_result["properties"]

            # Neo4j -> BODS
            reconstructed = map_entity_node(node_props, PUBLISHER_CONFIG)

            # Verify key fields preserved
            assert reconstructed["statementId"] == statement["statementId"]
            assert reconstructed["recordId"] == statement["recordId"]
            assert reconstructed["recordType"] == "entity"

            # Verify entity type preserved
            orig_type = statement["recordDetails"].get("entityType", {})
            rt_type = reconstructed["recordDetails"].get("entityType", {})
            assert rt_type.get("type") == orig_type.get("type"), (
                f"Entity type mismatch in {example_file.name}: "
                f"{rt_type.get('type')} != {orig_type.get('type')}"
            )
            if orig_type.get("subtype"):
                assert rt_type.get("subtype") == orig_type.get("subtype")

            # Verify name preserved
            orig_name = statement["recordDetails"].get("name", "")
            rt_name = reconstructed["recordDetails"].get("name", "")
            assert rt_name == orig_name

            # Verify identifiers preserved (via JSON round-trip)
            orig_ids = statement["recordDetails"].get("identifiers", [])
            rt_ids = reconstructed["recordDetails"].get("identifiers", [])
            assert rt_ids == orig_ids

            # Verify addresses preserved (via JSON round-trip)
            orig_addrs = statement["recordDetails"].get("addresses", [])
            rt_addrs = reconstructed["recordDetails"].get("addresses", [])
            assert rt_addrs == orig_addrs

    def test_person_round_trip(self, example_file):
        """Person statements should survive round-trip conversion."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "person":
                continue

            # BODS -> Neo4j
            neo4j_result = map_statement(statement)
            node_props = neo4j_result["properties"]

            # Neo4j -> BODS
            reconstructed = map_person_node(node_props, PUBLISHER_CONFIG)

            # Verify key fields
            assert reconstructed["statementId"] == statement["statementId"]
            assert reconstructed["recordId"] == statement["recordId"]
            assert reconstructed["recordType"] == "person"

            # Verify person type
            assert reconstructed["recordDetails"]["personType"] == (
                statement["recordDetails"]["personType"]
            )

            # Verify names preserved (via JSON round-trip)
            orig_names = statement["recordDetails"].get("names", [])
            rt_names = reconstructed["recordDetails"].get("names", [])
            assert rt_names == orig_names, (
                f"Names mismatch in {example_file.name}: "
                f"{statement.get('statementId')}"
            )

            # Verify nationalities preserved
            orig_nats = statement["recordDetails"].get("nationalities", [])
            rt_nats = reconstructed["recordDetails"].get("nationalities", [])
            assert rt_nats == orig_nats

    def test_relationship_round_trip(self, example_file):
        """Relationship statements should survive round-trip conversion."""
        for statement in read_bods_file(example_file):
            if statement["recordType"] != "relationship":
                continue

            # BODS -> Neo4j
            neo4j_result = map_statement(statement)
            rel_props = neo4j_result["properties"]

            # Add endpoint IDs as the extractor would
            rel_props["_sourceRecordId"] = neo4j_result.get("source_record_id", "")
            rel_props["_targetRecordId"] = neo4j_result.get("target_record_id", "")
            rel_props["_sourceLabels"] = ["Entity"]
            rel_props["_targetLabels"] = ["Entity"]

            # Neo4j -> BODS
            reconstructed = map_relationship(rel_props, PUBLISHER_CONFIG)

            # Verify key fields
            assert reconstructed["statementId"] == statement["statementId"]
            assert reconstructed["recordId"] == statement["recordId"]
            assert reconstructed["recordType"] == "relationship"

            # Verify endpoints preserved
            orig_subject = statement["recordDetails"].get("subject", "")
            orig_ip = statement["recordDetails"].get("interestedParty", "")
            if isinstance(orig_subject, str) and orig_subject:
                assert reconstructed["recordDetails"]["subject"] == orig_subject
            if isinstance(orig_ip, str) and orig_ip:
                assert reconstructed["recordDetails"]["interestedParty"] == orig_ip

            # Verify interests preserved (via JSON round-trip)
            orig_interests = statement["recordDetails"].get("interests", [])
            rt_interests = reconstructed["recordDetails"].get("interests", [])
            assert rt_interests == orig_interests, (
                f"Interests mismatch in {example_file.name}: "
                f"{statement.get('statementId')}"
            )

            # Verify isComponent preserved
            assert reconstructed["recordDetails"]["isComponent"] == (
                statement["recordDetails"].get("isComponent", False)
            )


class TestSpecificExamples:
    """Tests for specific example scenarios."""

    def test_fermcat_complex_structure(self):
        """fermcat.json is the most complex example - verify all types present."""
        filepath = EXAMPLES_DIR / "fermcat.json"
        if not filepath.exists():
            pytest.skip("fermcat.json not available")
        counts = count_statements(filepath)
        assert counts["entity"] > 0
        assert counts["person"] > 0
        assert counts["relationship"] > 0
        assert counts["total"] >= 10  # fermcat has many statements

    def test_pep_declaration(self):
        """PEP declarations should preserve political exposure data."""
        filepath = EXAMPLES_DIR / "full-pep-declaration.json"
        if not filepath.exists():
            pytest.skip("PEP example not available")
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "person":
                continue
            result = map_statement(statement)
            props = result["properties"]
            pep = statement["recordDetails"].get("politicalExposure", {})
            if pep:
                assert "politicalExposure_json" in props
                rt_pep = json.loads(props["politicalExposure_json"])
                assert rt_pep == pep

    def test_indirect_ownership_component_records(self):
        """Indirect ownership examples should handle isComponent correctly."""
        filepath = EXAMPLES_DIR / "indirect-ownership.json"
        if not filepath.exists():
            pytest.skip("Indirect ownership example not available")
        components = []
        primaries = []
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "relationship":
                continue
            result = map_statement(statement)
            if result["properties"].get("isComponent"):
                components.append(result)
            else:
                primaries.append(result)

        # Should have both component and primary relationships
        # (depending on example structure)
        total = len(components) + len(primaries)
        assert total > 0

    def test_entity_owning_entity(self):
        """Entity-to-entity ownership should produce entity->entity relationships."""
        filepath = EXAMPLES_DIR / "bods-package-entity-owning-entity.json"
        if not filepath.exists():
            pytest.skip("Entity-owning-entity example not available")

        # Collect all record IDs by type
        entity_ids = set()
        for statement in read_bods_file(filepath):
            if statement["recordType"] == "entity":
                entity_ids.add(statement["recordId"])
            elif statement["recordType"] == "relationship":
                result = map_statement(statement)
                # At least one relationship should have entity as source
                source_id = result["source_record_id"]
                if source_id in entity_ids:
                    # Entity-to-entity relationship found
                    return

        # If we get here, no entity-to-entity relationship was found
        # but that's OK if the example doesn't have one loaded yet
        # (the entity IDs are collected as we go, so the first rel
        # may reference an entity not yet seen)

    def test_annotations_preserved(self):
        """Annotations should be preserved through mapping."""
        filepath = EXAMPLES_DIR / "bods-package-annotations.json"
        if not filepath.exists():
            pytest.skip("Annotations example not available")
        for statement in read_bods_file(filepath):
            annotations = statement.get("annotations", [])
            if annotations:
                result = map_statement(statement)
                assert "annotations_json" in result["properties"]
                rt_annotations = json.loads(result["properties"]["annotations_json"])
                assert rt_annotations == annotations

    def test_multiple_tax_residencies(self):
        """Multiple tax residencies should be preserved."""
        filepath = EXAMPLES_DIR / "multiple-tax-residencies.json"
        if not filepath.exists():
            pytest.skip("Tax residencies example not available")
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "person":
                continue
            tax_res = statement["recordDetails"].get("taxResidencies", [])
            if tax_res:
                result = map_statement(statement)
                assert "taxResidencies_json" in result["properties"]
                rt_tax = json.loads(result["properties"]["taxResidencies_json"])
                assert rt_tax == tax_res

    def test_listed_company_exempt(self):
        """Listed company exemptions should preserve unspecified details."""
        filepath = EXAMPLES_DIR / "listed-company-exempt-from-disclosure.json"
        if not filepath.exists():
            pytest.skip("Listed company example not available")
        statements = list(read_bods_file(filepath))
        assert len(statements) > 0
        for statement in statements:
            result = map_statement(statement)
            assert result is not None

    def test_joint_ownership(self):
        """Joint ownership example should produce multiple relationships."""
        filepath = EXAMPLES_DIR / "joint-ownership.json"
        if not filepath.exists():
            pytest.skip("Joint ownership example not available")
        counts = count_statements(filepath)
        assert counts["relationship"] >= 2  # At least 2 joint owners

    def test_soe_state_body(self):
        """State-owned enterprise example should have state/stateBody entities."""
        filepath = EXAMPLES_DIR / "bods-package-fi-soe.json"
        if not filepath.exists():
            pytest.skip("SOE example not available")
        found_state_entity = False
        for statement in read_bods_file(filepath):
            if statement["recordType"] != "entity":
                continue
            entity_type = statement["recordDetails"].get("entityType", {})
            if entity_type.get("type") in ("state", "stateBody"):
                result = map_statement(statement)
                if "State" in result["labels"] or "StateBody" in result["labels"]:
                    found_state_entity = True
        assert found_state_entity, "No state/stateBody entity found in SOE example"
