"""Tests for the reverse Neo4j -> BODS mapper (round-trip fidelity)."""

from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.reader import read_bods_file
from bods_neo4j.config import PublisherConfig
from bods_neo4j.neo4j_to_bods.mapper import (
    map_entity_node,
    map_person_node,
    map_relationship,
)

from ._graph_state import GraphState

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


@pytest.fixture
def publisher_config():
    return PublisherConfig(publisher_name="Test Publisher", bods_version="0.4")


@pytest.fixture
def graph_state():
    return GraphState.from_statements(read_bods_file(SAMPLE_FILE))


@pytest.fixture
def statements():
    return list(read_bods_file(SAMPLE_FILE))


class TestEntityRoundTrip:
    def test_preserves_name_and_ids(self, graph_state, publisher_config):
        env = graph_state.entity_envelope("rec-entity-alpha")
        out = map_entity_node(env, publisher_config)
        assert out["recordId"] == "rec-entity-alpha"
        assert out["statementId"] == "test-entity-001"
        assert out["recordDetails"]["name"] == "Alpha Corp"

    def test_preserves_identifiers(self, graph_state, statements, publisher_config):
        env = graph_state.entity_envelope("rec-entity-alpha")
        out = map_entity_node(env, publisher_config)
        original = next(s for s in statements if s["recordId"] == "rec-entity-alpha")
        assert out["recordDetails"]["identifiers"] == original["recordDetails"]["identifiers"]

    def test_preserves_jurisdiction(self, graph_state, publisher_config):
        env = graph_state.entity_envelope("rec-entity-alpha")
        out = map_entity_node(env, publisher_config)
        assert out["recordDetails"]["jurisdiction"]["code"] == "GB"
        assert out["recordDetails"]["jurisdiction"]["name"] == "United Kingdom"

    def test_preserves_entity_type_and_subtype(self, graph_state, publisher_config):
        env = graph_state.entity_envelope("rec-entity-trust")
        out = map_entity_node(env, publisher_config)
        assert out["recordDetails"]["entityType"]["type"] == "arrangement"
        assert out["recordDetails"]["entityType"]["subtype"] == "trust"

    def test_preserves_addresses(self, graph_state, statements, publisher_config):
        env = graph_state.entity_envelope("rec-entity-alpha")
        out = map_entity_node(env, publisher_config)
        original = next(s for s in statements if s["recordId"] == "rec-entity-alpha")
        assert out["recordDetails"]["addresses"] == original["recordDetails"]["addresses"]


class TestPersonRoundTrip:
    def test_preserves_name(self, graph_state, publisher_config):
        env = graph_state.person_envelope("rec-person-alice")
        out = map_person_node(env, publisher_config)
        assert out["recordId"] == "rec-person-alice"
        assert out["recordDetails"]["names"][0]["fullName"] == "Alice Johnson"

    def test_preserves_nationalities(self, graph_state, statements, publisher_config):
        env = graph_state.person_envelope("rec-person-alice")
        out = map_person_node(env, publisher_config)
        original = next(s for s in statements if s["recordId"] == "rec-person-alice")
        assert out["recordDetails"]["nationalities"] == original["recordDetails"]["nationalities"]


class TestRelationshipRoundTrip:
    def test_preserves_interests(self, graph_state, statements, publisher_config):
        env = graph_state.relationship_envelope("test-rel-001")
        out = map_relationship(env, publisher_config)
        original = next(s for s in statements if s["statementId"] == "test-rel-001")
        assert out["recordDetails"]["interests"] == original["recordDetails"]["interests"]

    def test_preserves_endpoints(self, graph_state, publisher_config):
        env = graph_state.relationship_envelope("test-rel-001")
        out = map_relationship(env, publisher_config)
        assert out["recordDetails"]["subject"] == "rec-entity-alpha"
        assert out["recordDetails"]["interestedParty"] == "rec-person-alice"
