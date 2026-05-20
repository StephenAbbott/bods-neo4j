"""Tests for the forward BODS -> Neo4j mapper (graph-native shape)."""

import json
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.mapper import map_statement
from bods_neo4j.bods_to_neo4j.reader import read_bods_file

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


def _nodes_by_label(graph, label):
    return [n for n in graph["nodes"] if label in n["labels"]]


def _edges_by_type(graph, rel_type):
    return [e for e in graph["edges"] if e["rel_type"] == rel_type]


def _primary(graph):
    """Return the primary node spec — the first node in the graph's node list."""
    return graph["nodes"][0]


@pytest.fixture
def all_statements():
    return list(read_bods_file(SAMPLE_FILE))


@pytest.fixture
def entity_statements(all_statements):
    return [s for s in all_statements if s["recordType"] == "entity"]


@pytest.fixture
def person_statements(all_statements):
    return [s for s in all_statements if s["recordType"] == "person"]


@pytest.fixture
def relationship_statements(all_statements):
    return [s for s in all_statements if s["recordType"] == "relationship"]


class TestMapEntityStatement:
    def test_emits_entity_node_with_subtype_labels(self, entity_statements):
        graph = map_statement(entity_statements[0])  # Alpha Corp
        assert graph["statement_type"] == "entity"
        entity_nodes = _nodes_by_label(graph, "Entity")
        assert len(entity_nodes) == 1
        assert "RegisteredEntity" in entity_nodes[0]["labels"]

    def test_entity_scalar_properties(self, entity_statements):
        graph = map_statement(entity_statements[0])
        props = _primary(graph)["properties"]
        assert props["statementId"] == "test-entity-001"
        assert props["recordId"] == "rec-entity-alpha"
        assert props["name"] == "Alpha Corp"
        assert props["entityType"] == "registeredEntity"
        assert props["foundingDate"] == "2020-01-01"

    def test_jurisdiction_becomes_country_node_and_edge(self, entity_statements):
        graph = map_statement(entity_statements[0])
        countries = _nodes_by_label(graph, "Country")
        assert any(c["key_value"] == "GB" for c in countries)
        reg = _edges_by_type(graph, "REGISTERED_IN")
        assert len(reg) == 1
        assert reg[0]["end_key_value"] == "GB"

    def test_identifiers_extracted_as_nodes(self, entity_statements):
        graph = map_statement(entity_statements[0])
        idents = _nodes_by_label(graph, "Identifier")
        assert len(idents) == 1
        assert idents[0]["properties"]["scheme"] == "GB-COH"
        assert idents[0]["properties"]["id"] == "00112233"
        # And a HAS_IDENTIFIER edge from Entity to Identifier.
        has_ident = _edges_by_type(graph, "HAS_IDENTIFIER")
        assert len(has_ident) == 1
        assert has_ident[0]["properties"]["isPrimary"] is True

    def test_addresses_become_address_nodes(self, entity_statements):
        graph = map_statement(entity_statements[0])  # Alpha Corp has 1 registered address
        addrs = _nodes_by_label(graph, "Address")
        assert len(addrs) == 1
        assert addrs[0]["properties"]["address"] == "1 Test St, London"
        assert addrs[0]["properties"]["postCode"] == "SW1A 1AA"
        # HAS_ADDRESS edge carries the BODS address type and ordinal
        has_addr = _edges_by_type(graph, "HAS_ADDRESS")
        assert has_addr[0]["properties"]["type"] == "registered"

    def test_trust_entity_labels(self, entity_statements):
        graph = map_statement(entity_statements[2])  # Alpha Family Trust
        labels = set(_primary(graph)["labels"])
        assert {"Entity", "Arrangement", "Trust"}.issubset(labels)

    def test_no_legacy_json_properties(self, entity_statements):
        """No `*_json` blob properties should appear on the primary entity
        node — they are replaced by separate Identifier/Address/Country nodes."""
        graph = map_statement(entity_statements[0])
        props = _primary(graph)["properties"]
        for key in props:
            assert not key.endswith("_json"), (
                f"Legacy JSON property leaked onto Entity: {key}"
            )

    def test_publication_details_stay_inline(self, entity_statements):
        graph = map_statement(entity_statements[0])
        props = _primary(graph)["properties"]
        assert props["publisherName"] == "Test Publisher"
        assert props["publicationDate"] == "2024-01-15"
        assert props["bodsVersion"] == "0.4"


class TestMapPersonStatement:
    def test_emits_person_node(self, person_statements):
        graph = map_statement(person_statements[0])
        assert graph["statement_type"] == "person"
        assert _primary(graph)["labels"] == ["Person"]

    def test_person_scalar_properties(self, person_statements):
        graph = map_statement(person_statements[0])
        props = _primary(graph)["properties"]
        assert props["recordId"] == "rec-person-alice"
        assert props["personType"] == "knownPerson"
        assert props["givenName"] == "Alice"
        assert props["familyName"] == "Johnson"
        assert props["birthDate"] == "1980-03"

    def test_nationalities_become_inline_parallel_lists(self, person_statements):
        graph = map_statement(person_statements[0])
        props = _primary(graph)["properties"]
        assert props["nationalityCodes"] == ["GB"]
        assert props["nationalityNames"] == ["United Kingdom"]

    def test_address_emitted_for_person_with_residence(self, person_statements):
        graph = map_statement(person_statements[0])  # Alice has a residence address
        addrs = _nodes_by_label(graph, "Address")
        assert len(addrs) == 1
        has_addr = _edges_by_type(graph, "HAS_ADDRESS")
        assert has_addr[0]["properties"]["type"] == "residence"


class TestMapRelationshipStatement:
    def test_emits_typed_edge_for_single_interest(self, relationship_statements):
        # test-rel-001 has 1 interest (shareholding)
        graph = map_statement(relationship_statements[0])
        assert graph["statement_type"] == "relationship"
        owns = _edges_by_type(graph, "OWNS")
        assert len(owns) == 1
        e = owns[0]
        assert e["start_key_value"] == "rec-person-alice"
        assert e["end_key_value"] == "rec-entity-alpha"
        assert e["properties"]["bodsInterestType"] == "shareholding"
        assert e["properties"]["family"] == "OWNS"

    def test_share_extracted_to_edge_properties(self, relationship_statements):
        graph = map_statement(relationship_statements[0])
        e = _edges_by_type(graph, "OWNS")[0]
        assert e["properties"]["shareMinimum"] == 50.0
        assert e["properties"]["shareMaximum"] == 75.0
        assert e["properties"]["directOrIndirect"] == "direct"
        assert e["properties"]["beneficialOwnershipOrControl"] is True

    def test_exact_share(self, relationship_statements):
        graph = map_statement(relationship_statements[2])  # Bob -> Beta, exact: 100
        e = _edges_by_type(graph, "OWNS")[0]
        assert e["properties"]["shareExact"] == 100.0

    def test_envelope_carries_endpoint_record_ids(self, relationship_statements):
        graph = map_statement(relationship_statements[0])
        assert graph["interested_party_record_id"] == "rec-person-alice"
        assert graph["subject_record_id"] == "rec-entity-alpha"

    def test_no_interest_nodes_emitted(self, relationship_statements):
        """Interests are carried on edges; no :Interest nodes should exist."""
        graph = map_statement(relationship_statements[0])
        interest_nodes = _nodes_by_label(graph, "Interest")
        assert interest_nodes == []


class TestDetailsCategoryValueParsing:
    """The forward mapper splits structured `"<Category>: <Value>"` strings
    in the interest `details` field into `detailsCategory` / `detailsValue`
    properties on the edge. Producer-agnostic — fires on any BODS source
    that follows the convention (GLEIF, UK PSC, ...). The original
    `details` string stays intact so the round-trip is lossless."""

    def _stmt_with_interest(self, **interest):
        return {
            "statementId": "s1",
            "recordId": "rec-rel",
            "recordType": "relationship",
            "recordDetails": {
                "subject": "rec-target",
                "interestedParty": "rec-party",
                "interests": [interest],
            },
        }

    def test_relationship_type_parsed(self):
        stmt = self._stmt_with_interest(
            type="otherInfluenceOrControl",
            details="Relationship Type: IS_ULTIMATELY_CONSOLIDATED_BY",
        )
        e = map_statement(stmt)["edges"][0]
        assert e["properties"]["details"] == "Relationship Type: IS_ULTIMATELY_CONSOLIDATED_BY"
        assert e["properties"]["detailsCategory"] == "Relationship Type"
        assert e["properties"]["detailsValue"] == "IS_ULTIMATELY_CONSOLIDATED_BY"

    def test_exception_category_parsed(self):
        stmt = self._stmt_with_interest(
            type="otherInfluenceOrControl",
            details="Exception Category: ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
        )
        e = map_statement(stmt)["edges"][0]
        assert e["properties"]["detailsCategory"] == "Exception Category"
        assert e["properties"]["detailsValue"] == "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT"

    def test_uk_psc_relationship_type_also_parses(self):
        # UK PSC uses the same convention with a different value vocabulary
        stmt = self._stmt_with_interest(
            type="otherInfluenceOrControl",
            details="Relationship Type: persons-with-significant-control-statement",
        )
        e = map_statement(stmt)["edges"][0]
        assert e["properties"]["detailsCategory"] == "Relationship Type"
        assert e["properties"]["detailsValue"] == "persons-with-significant-control-statement"

    def test_unrecognised_details_is_left_intact(self):
        stmt = self._stmt_with_interest(
            type="otherInfluenceOrControl",
            details="some free-form text the producer chose",
        )
        e = map_statement(stmt)["edges"][0]
        assert e["properties"]["details"] == "some free-form text the producer chose"
        assert "detailsCategory" not in e["properties"]
        assert "detailsValue" not in e["properties"]

    def test_lowercase_prefix_not_parsed(self):
        # Free-form prose with a colon shouldn't be treated as structured.
        stmt = self._stmt_with_interest(
            type="otherInfluenceOrControl",
            details="note: held since 2019",
        )
        e = map_statement(stmt)["edges"][0]
        assert "detailsCategory" not in e["properties"]
        assert "detailsValue" not in e["properties"]


class TestMultiInterestRelationship:
    """Sample data has stmt-rel-001 with 2 interests (shareholding + votingRights)."""

    def test_two_interests_become_two_typed_edges(self):
        sample = Path(__file__).parent.parent / "examples" / "sample_data" / "sample_bods.json"
        if not sample.exists():
            pytest.skip("examples/sample_data/sample_bods.json not present")
        statements = list(read_bods_file(sample))
        rel = next(s for s in statements if s["statementId"] == "stmt-rel-001")
        graph = map_statement(rel)
        # Two edges from Jane to Acme, one OWNS (shareholding) + one CONTROLS (votingRights)
        edges_from_jane = [
            e for e in graph["edges"]
            if e["start_key_value"] == "rec-person-jane"
        ]
        assert len(edges_from_jane) == 2
        rel_types = {e["rel_type"] for e in edges_from_jane}
        assert rel_types == {"OWNS", "CONTROLS"}
        types_on_edges = {e["properties"]["bodsInterestType"] for e in edges_from_jane}
        assert types_on_edges == {"shareholding", "votingRights"}
        # Both edges share the same statementId
        sids = {e["properties"]["statementId"] for e in edges_from_jane}
        assert sids == {"stmt-rel-001"}


class TestMapUnknownStatement:
    def test_unknown_record_type(self):
        assert map_statement({"recordType": "unknown", "statementId": "x"}) is None

    def test_missing_record_type(self):
        assert map_statement({"statementId": "x"}) is None
