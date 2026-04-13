"""Tests for BODS schema utilities."""

import pytest

from bods_neo4j.utils.bods_schema import (
    get_record_type,
    get_record_details,
    get_entity_type,
    get_entity_subtype,
    get_person_type,
    get_neo4j_labels_for_entity,
    extract_primary_name,
    extract_identifiers,
    extract_interests,
    RECORD_TYPE_ENTITY,
    RECORD_TYPE_PERSON,
    RECORD_TYPE_RELATIONSHIP,
)


class TestGetRecordType:
    def test_entity(self):
        assert get_record_type({"recordType": "entity"}) == "entity"

    def test_person(self):
        assert get_record_type({"recordType": "person"}) == "person"

    def test_relationship(self):
        assert get_record_type({"recordType": "relationship"}) == "relationship"

    def test_missing(self):
        assert get_record_type({}) == ""


class TestGetNeo4jLabels:
    def test_registered_entity(self):
        details = {"entityType": {"type": "registeredEntity"}}
        labels = get_neo4j_labels_for_entity(details)
        assert "Entity" in labels
        assert "RegisteredEntity" in labels

    def test_trust(self):
        details = {"entityType": {"type": "arrangement", "subtype": "trust"}}
        labels = get_neo4j_labels_for_entity(details)
        assert "Entity" in labels
        assert "Arrangement" in labels
        assert "Trust" in labels

    def test_state_body(self):
        details = {"entityType": {"type": "stateBody", "subtype": "governmentDepartment"}}
        labels = get_neo4j_labels_for_entity(details)
        assert "Entity" in labels
        assert "StateBody" in labels
        assert "GovernmentDepartment" in labels

    def test_nomination(self):
        details = {"entityType": {"type": "arrangement", "subtype": "nomination"}}
        labels = get_neo4j_labels_for_entity(details)
        assert "Nomination" in labels

    def test_unknown_type(self):
        details = {"entityType": {"type": "unknownType"}}
        labels = get_neo4j_labels_for_entity(details)
        assert labels == ["Entity"]  # Only base label

    def test_empty_details(self):
        labels = get_neo4j_labels_for_entity({})
        assert labels == ["Entity"]


class TestExtractPrimaryName:
    def test_entity_name(self):
        details = {"name": "Acme Corp"}
        assert extract_primary_name(details, RECORD_TYPE_ENTITY) == "Acme Corp"

    def test_person_full_name(self):
        details = {"names": [{"fullName": "Jane Smith"}]}
        assert extract_primary_name(details, RECORD_TYPE_PERSON) == "Jane Smith"

    def test_person_name_parts(self):
        details = {"names": [{"givenName": "Jane", "familyName": "Smith"}]}
        assert extract_primary_name(details, RECORD_TYPE_PERSON) == "Jane Smith"

    def test_person_no_names(self):
        details = {"names": []}
        assert extract_primary_name(details, RECORD_TYPE_PERSON) == ""

    def test_empty(self):
        assert extract_primary_name({}, RECORD_TYPE_ENTITY) == ""


class TestExtractInterests:
    def test_with_interests(self):
        details = {"interests": [{"type": "shareholding"}]}
        assert extract_interests(details) == [{"type": "shareholding"}]

    def test_no_interests(self):
        assert extract_interests({}) == []
