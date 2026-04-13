"""Tests for BODS to Neo4j mapper."""

import json
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.bods_to_neo4j.reader import read_bods_file

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


@pytest.fixture
def all_statements():
    """Load all test statements."""
    return list(read_bods_file(SAMPLE_FILE))


@pytest.fixture
def entity_statements(all_statements):
    """Filter to entity statements only."""
    return [s for s in all_statements if s["recordType"] == "entity"]


@pytest.fixture
def person_statements(all_statements):
    """Filter to person statements only."""
    return [s for s in all_statements if s["recordType"] == "person"]


@pytest.fixture
def relationship_statements(all_statements):
    """Filter to relationship statements only."""
    return [s for s in all_statements if s["recordType"] == "relationship"]


class TestMapEntityStatement:
    """Tests for entity statement mapping."""

    def test_basic_entity_mapping(self, entity_statements):
        """Entity statement maps to a node with Entity label."""
        statement = entity_statements[0]  # Alpha Corp
        result = map_statement(statement)

        assert result is not None
        assert result["type"] == "node"
        assert "Entity" in result["labels"]
        assert "RegisteredEntity" in result["labels"]

    def test_entity_properties(self, entity_statements):
        """Entity properties are correctly mapped."""
        statement = entity_statements[0]  # Alpha Corp
        result = map_statement(statement)
        props = result["properties"]

        assert props["statementId"] == "test-entity-001"
        assert props["recordId"] == "rec-entity-alpha"
        assert props["recordType"] == "entity"
        assert props["name"] == "Alpha Corp"
        assert props["entityType"] == "registeredEntity"
        assert props["jurisdictionCode"] == "GB"
        assert props["jurisdictionName"] == "United Kingdom"
        assert props["foundingDate"] == "2020-01-01"

    def test_entity_identifiers(self, entity_statements):
        """Entity identifiers are preserved as JSON and primary fields."""
        statement = entity_statements[0]  # Alpha Corp
        result = map_statement(statement)
        props = result["properties"]

        assert props["primaryIdentifierId"] == "00112233"
        assert props["primaryIdentifierScheme"] == "GB-COH"
        assert "identifiers_json" in props
        identifiers = json.loads(props["identifiers_json"])
        assert len(identifiers) == 1
        assert identifiers[0]["scheme"] == "GB-COH"

    def test_entity_addresses(self, entity_statements):
        """Entity addresses are preserved as JSON and summary fields."""
        statement = entity_statements[0]  # Alpha Corp
        result = map_statement(statement)
        props = result["properties"]

        assert props["registeredAddress"] == "1 Test St, London"
        assert props["registeredPostCode"] == "SW1A 1AA"
        assert props["registeredCountry"] == "GB"
        assert "addresses_json" in props

    def test_trust_entity_labels(self, entity_statements):
        """Trust entity gets Arrangement and Trust labels."""
        statement = entity_statements[2]  # The Alpha Family Trust
        result = map_statement(statement)

        assert "Entity" in result["labels"]
        assert "Arrangement" in result["labels"]
        assert "Trust" in result["labels"]

    def test_entity_publication_details(self, entity_statements):
        """Publication details are preserved."""
        statement = entity_statements[0]
        result = map_statement(statement)
        props = result["properties"]

        assert props["publisherName"] == "Test Publisher"
        assert props["publicationDate"] == "2024-01-15"
        assert props["bodsVersion"] == "0.4"

    def test_empty_strings_removed(self, entity_statements):
        """Empty string values are not included in properties."""
        statement = entity_statements[1]  # Beta Holdings (no address)
        result = map_statement(statement)
        props = result["properties"]

        assert "registeredAddress" not in props
        assert "dissolutionDate" not in props


class TestMapPersonStatement:
    """Tests for person statement mapping."""

    def test_basic_person_mapping(self, person_statements):
        """Person statement maps to a node with Person label."""
        statement = person_statements[0]  # Alice Johnson
        result = map_statement(statement)

        assert result is not None
        assert result["type"] == "node"
        assert result["labels"] == ["Person"]

    def test_person_properties(self, person_statements):
        """Person properties are correctly mapped."""
        statement = person_statements[0]  # Alice Johnson
        result = map_statement(statement)
        props = result["properties"]

        assert props["statementId"] == "test-person-001"
        assert props["recordId"] == "rec-person-alice"
        assert props["recordType"] == "person"
        assert props["name"] == "Alice Johnson"
        assert props["personType"] == "knownPerson"
        assert props["givenName"] == "Alice"
        assert props["familyName"] == "Johnson"
        assert props["birthDate"] == "1980-03"
        assert props["nationalityCode"] == "GB"

    def test_person_names_json(self, person_statements):
        """Person names array is preserved as JSON."""
        statement = person_statements[0]  # Alice Johnson
        result = map_statement(statement)
        props = result["properties"]

        assert "names_json" in props
        names = json.loads(props["names_json"])
        assert len(names) == 1
        assert names[0]["fullName"] == "Alice Johnson"
        assert names[0]["type"] == "legal"


class TestMapRelationshipStatement:
    """Tests for relationship statement mapping."""

    def test_basic_relationship_mapping(self, relationship_statements):
        """Relationship statement maps to a relationship dict."""
        statement = relationship_statements[0]  # Alice -> Alpha Corp
        result = map_statement(statement)

        assert result is not None
        assert result["type"] == "relationship"
        assert result["rel_type"] == "HAS_INTEREST"

    def test_relationship_endpoints(self, relationship_statements):
        """Relationship source and target are correctly mapped."""
        statement = relationship_statements[0]  # Alice -> Alpha Corp
        result = map_statement(statement)

        assert result["source_record_id"] == "rec-person-alice"
        assert result["target_record_id"] == "rec-entity-alpha"

    def test_relationship_properties(self, relationship_statements):
        """Relationship properties include interest details."""
        statement = relationship_statements[0]  # Alice -> Alpha Corp
        result = map_statement(statement)
        props = result["properties"]

        assert props["statementId"] == "test-rel-001"
        assert props["recordId"] == "rec-rel-alice-alpha"
        assert props["isBeneficialOwnership"] is True
        assert props["directOrIndirect"] == "direct"
        assert props["shareMinimum"] == 50
        assert props["shareMaximum"] == 75
        assert props["interestStartDate"] == "2020-01-01"

    def test_relationship_interest_types(self, relationship_statements):
        """Interest types are extracted as a list."""
        statement = relationship_statements[0]  # Alice -> Alpha Corp
        result = map_statement(statement)
        props = result["properties"]

        assert "shareholding" in props["interestTypes"]

    def test_relationship_interests_json(self, relationship_statements):
        """Full interests array is preserved as JSON."""
        statement = relationship_statements[0]
        result = map_statement(statement)
        props = result["properties"]

        assert "interests_json" in props
        interests = json.loads(props["interests_json"])
        assert len(interests) == 1
        assert interests[0]["type"] == "shareholding"

    def test_entity_to_entity_relationship(self, relationship_statements):
        """Entity-to-entity ownership is correctly mapped."""
        statement = relationship_statements[1]  # Beta -> Alpha
        result = map_statement(statement)

        assert result["source_record_id"] == "rec-entity-beta"
        assert result["target_record_id"] == "rec-entity-alpha"

    def test_exact_share(self, relationship_statements):
        """Exact share values are captured."""
        statement = relationship_statements[2]  # Bob -> Beta (100%)
        result = map_statement(statement)
        props = result["properties"]

        assert props["shareExact"] == 100


class TestMapUnknownStatement:
    """Tests for edge cases."""

    def test_unknown_record_type(self):
        """Unknown record type returns None."""
        statement = {"recordType": "unknown", "statementId": "test"}
        result = map_statement(statement)
        assert result is None

    def test_missing_record_type(self):
        """Missing record type returns None."""
        statement = {"statementId": "test"}
        result = map_statement(statement)
        assert result is None
