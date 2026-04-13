"""Tests for Neo4j to BODS mapper (round-trip fidelity)."""

import json
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.bods_to_neo4j.reader import read_bods_file
from bods_neo4j.neo4j_to_bods.mapper import (
    map_entity_node,
    map_person_node,
    map_relationship,
)
from bods_neo4j.config import PublisherConfig

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


@pytest.fixture
def publisher_config():
    return PublisherConfig(publisher_name="Test Publisher", bods_version="0.4")


class TestEntityRoundTrip:
    """Test that entity statements survive BODS -> Neo4j -> BODS conversion."""

    def test_entity_round_trip_preserves_name(self, publisher_config):
        """Entity name survives round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        entity_stmt = statements[0]  # Alpha Corp

        # BODS -> Neo4j node
        neo4j_node = map_statement(entity_stmt)
        assert neo4j_node is not None
        node_props = neo4j_node["properties"]

        # Neo4j node -> BODS
        reconstructed = map_entity_node(node_props, publisher_config)

        assert reconstructed["recordDetails"]["name"] == "Alpha Corp"
        assert reconstructed["recordId"] == "rec-entity-alpha"
        assert reconstructed["statementId"] == "test-entity-001"

    def test_entity_round_trip_preserves_identifiers(self, publisher_config):
        """Entity identifiers survive round-trip via JSON serialisation."""
        statements = list(read_bods_file(SAMPLE_FILE))
        entity_stmt = statements[0]  # Alpha Corp

        neo4j_node = map_statement(entity_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_entity_node(node_props, publisher_config)

        original_ids = entity_stmt["recordDetails"]["identifiers"]
        round_trip_ids = reconstructed["recordDetails"]["identifiers"]
        assert round_trip_ids == original_ids

    def test_entity_round_trip_preserves_jurisdiction(self, publisher_config):
        """Entity jurisdiction survives round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        entity_stmt = statements[0]  # Alpha Corp

        neo4j_node = map_statement(entity_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_entity_node(node_props, publisher_config)

        assert reconstructed["recordDetails"]["jurisdiction"]["code"] == "GB"
        assert reconstructed["recordDetails"]["jurisdiction"]["name"] == "United Kingdom"

    def test_entity_round_trip_preserves_type(self, publisher_config):
        """Entity type and subtype survive round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        trust_stmt = statements[2]  # The Alpha Family Trust

        neo4j_node = map_statement(trust_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_entity_node(node_props, publisher_config)

        assert reconstructed["recordDetails"]["entityType"]["type"] == "arrangement"
        assert reconstructed["recordDetails"]["entityType"]["subtype"] == "trust"

    def test_entity_round_trip_preserves_addresses(self, publisher_config):
        """Entity addresses survive round-trip via JSON serialisation."""
        statements = list(read_bods_file(SAMPLE_FILE))
        entity_stmt = statements[0]  # Alpha Corp

        neo4j_node = map_statement(entity_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_entity_node(node_props, publisher_config)

        original_addrs = entity_stmt["recordDetails"]["addresses"]
        round_trip_addrs = reconstructed["recordDetails"]["addresses"]
        assert round_trip_addrs == original_addrs


class TestPersonRoundTrip:
    """Test that person statements survive BODS -> Neo4j -> BODS conversion."""

    def test_person_round_trip_preserves_name(self, publisher_config):
        """Person name survives round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        person_stmt = statements[3]  # Alice Johnson

        neo4j_node = map_statement(person_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_person_node(node_props, publisher_config)

        assert reconstructed["recordDetails"]["names"][0]["fullName"] == "Alice Johnson"
        assert reconstructed["recordId"] == "rec-person-alice"

    def test_person_round_trip_preserves_nationalities(self, publisher_config):
        """Person nationalities survive round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        person_stmt = statements[3]  # Alice Johnson

        neo4j_node = map_statement(person_stmt)
        node_props = neo4j_node["properties"]
        reconstructed = map_person_node(node_props, publisher_config)

        original_nats = person_stmt["recordDetails"]["nationalities"]
        round_trip_nats = reconstructed["recordDetails"]["nationalities"]
        assert round_trip_nats == original_nats


class TestRelationshipRoundTrip:
    """Test that relationship statements survive BODS -> Neo4j -> BODS conversion."""

    def test_relationship_round_trip_preserves_interests(self, publisher_config):
        """Relationship interests survive round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        rel_stmt = statements[5]  # Alice -> Alpha Corp

        neo4j_rel = map_statement(rel_stmt)
        rel_props = neo4j_rel["properties"]
        # Add endpoint IDs as the extractor would
        rel_props["_sourceRecordId"] = neo4j_rel["source_record_id"]
        rel_props["_targetRecordId"] = neo4j_rel["target_record_id"]
        rel_props["_sourceLabels"] = ["Person"]
        rel_props["_targetLabels"] = ["Entity"]

        reconstructed = map_relationship(rel_props, publisher_config)

        original_interests = rel_stmt["recordDetails"]["interests"]
        round_trip_interests = reconstructed["recordDetails"]["interests"]
        assert round_trip_interests == original_interests

    def test_relationship_round_trip_preserves_endpoints(self, publisher_config):
        """Relationship subject and interestedParty survive round-trip."""
        statements = list(read_bods_file(SAMPLE_FILE))
        rel_stmt = statements[5]  # Alice -> Alpha Corp

        neo4j_rel = map_statement(rel_stmt)
        rel_props = neo4j_rel["properties"]
        rel_props["_sourceRecordId"] = neo4j_rel["source_record_id"]
        rel_props["_targetRecordId"] = neo4j_rel["target_record_id"]
        rel_props["_sourceLabels"] = ["Person"]
        rel_props["_targetLabels"] = ["Entity"]

        reconstructed = map_relationship(rel_props, publisher_config)

        assert reconstructed["recordDetails"]["subject"] == "rec-entity-alpha"
        assert reconstructed["recordDetails"]["interestedParty"] == "rec-person-alice"
