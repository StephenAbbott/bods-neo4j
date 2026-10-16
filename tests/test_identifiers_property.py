"""Tests for the opt-in denormalised identifiers property (issue #3)."""

from bods_neo4j.bods_to_neo4j.mapper import (
    add_identifiers_property,
    identifier_token,
    map_statement,
)


def _entity(identifiers):
    return {
        "statementId": "s1", "recordId": "rec-1", "recordType": "entity",
        "statementDate": "2024-01-01",
        "recordDetails": {
            "isComponent": False,
            "entityType": {"type": "registeredEntity"},
            "name": "Alpha Ltd",
            "identifiers": identifiers,
        },
    }


def _node(graph, label):
    return next(n for n in graph["nodes"] if n["labels"][0] == label)


def test_token_lowercases_scheme_keeps_id_exact():
    assert identifier_token("GB-COH", "SC123abc") == "gb-coh:SC123abc"
    assert identifier_token("", "123") is None
    assert identifier_token("XI-LEI", None) is None


def test_statement_order_kept_duplicates_and_schemeless_dropped():
    graph = map_statement(_entity([
        {"scheme": "XI-LEI", "id": "5493001KJTIIGC8Y1R12"},
        {"scheme": "GB-COH", "id": "00041424"},
        {"id": "no-scheme"},
        {"scheme": "XI-LEI", "id": "5493001KJTIIGC8Y1R12"},
    ]))
    add_identifiers_property(graph)
    assert _node(graph, "Entity")["properties"]["identifiers"] == [
        "xi-lei:5493001KJTIIGC8Y1R12", "gb-coh:00041424",
    ]


def test_schemename_used_when_no_scheme():
    graph = map_statement(_entity([{"schemeName": "Companies House", "id": "1"}]))
    add_identifiers_property(graph)
    assert _node(graph, "Entity")["properties"]["identifiers"] == ["companies house:1"]


def test_no_identifiers_no_property():
    graph = map_statement(_entity([]))
    add_identifiers_property(graph)
    assert "identifiers" not in _node(graph, "Entity")["properties"]


def test_not_added_unless_asked():
    graph = map_statement(_entity([{"scheme": "GB-COH", "id": "1"}]))
    assert "identifiers" not in _node(graph, "Entity")["properties"]
