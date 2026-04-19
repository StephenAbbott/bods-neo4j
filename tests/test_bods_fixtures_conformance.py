"""Conformance tests against the shared bods-fixtures pack.

The pack (https://github.com/StephenAbbott/bods-fixtures) is the canonical
source of truth for BODS v0.4 shape across the adapter ecosystem. Passing
these tests means bods-neo4j's mapper agrees with the canonical envelope
that other adapters also target. Failures here indicate either genuine
bugs in mapping or a fixture-pack bug worth reporting upstream.

Graph-specific concerns tested here:
- circular ownership must produce two distinct HAS_INTEREST edges (one
  per cycle direction), not be deduplicated or looped forever.
- declared-unknown UBOs (inline ``unspecifiedReason`` objects as
  ``interestedParty``) must not crash the mapper and must leave at least
  one usable node in the graph for the known subject.

The ``bods_fixture`` parameter is auto-parametrized by the
pytest-bods-fixtures plugin over every case in the pack. Tests that need
a specific case use ``load(name)`` directly.
"""

from __future__ import annotations

from bods_fixtures import Fixture, load

from bods_neo4j.bods_to_neo4j.mapper import map_statement


def test_every_statement_maps(bods_fixture: Fixture) -> None:
    """Every statement in every canonical fixture must map to a node or edge.
    None returns here mean the mapper failed to recognise canonical v0.4
    shape — usually a sign it's reading v0.3 fields."""
    for stmt in bods_fixture.statements:
        mapped = map_statement(stmt)
        assert mapped is not None, (
            f"{bods_fixture.name}: map_statement returned None for "
            f"recordType={stmt.get('recordType')} statementId={stmt.get('statementId')}"
        )
        assert mapped["type"] in {"node", "relationship"}, (
            f"unexpected mapped type {mapped['type']!r}"
        )


def test_node_and_edge_counts_match_fixture(bods_fixture: Fixture) -> None:
    """Entity + person records must produce nodes; relationship records
    must produce HAS_INTEREST edges. A count mismatch means something is
    being silently dropped or misrouted by record type."""
    nodes = 0
    edges = 0
    for stmt in bods_fixture.statements:
        mapped = map_statement(stmt)
        assert mapped is not None
        if mapped["type"] == "node":
            nodes += 1
        else:
            edges += 1

    expected_nodes = len(bods_fixture.by_record_type("entity")) + len(
        bods_fixture.by_record_type("person")
    )
    expected_edges = len(bods_fixture.by_record_type("relationship"))
    assert nodes == expected_nodes, (
        f"{bods_fixture.name}: {nodes} nodes emitted, expected {expected_nodes}"
    )
    assert edges == expected_edges, (
        f"{bods_fixture.name}: {edges} edges emitted, expected {expected_edges}"
    )


def test_direct_ownership_produces_entity_person_and_edge() -> None:
    """The baseline fixture must produce exactly one Entity node, one
    Person node, and one HAS_INTEREST edge wired between them."""
    fixture = load("core/01-direct-ownership")
    mapped = [map_statement(s) for s in fixture.statements]

    entity_nodes = [
        m for m in mapped if m and m["type"] == "node" and "Entity" in m["labels"]
    ]
    person_nodes = [
        m for m in mapped if m and m["type"] == "node" and "Person" in m["labels"]
    ]
    edges = [m for m in mapped if m and m["type"] == "relationship"]

    assert len(entity_nodes) == 1, f"expected 1 Entity node, got {len(entity_nodes)}"
    assert len(person_nodes) == 1, f"expected 1 Person node, got {len(person_nodes)}"
    assert len(edges) == 1, f"expected 1 edge, got {len(edges)}"

    # The edge must wire the person → entity (interestedParty → subject).
    edge = edges[0]
    entity_record_id = entity_nodes[0]["properties"]["recordId"]
    person_record_id = person_nodes[0]["properties"]["recordId"]
    assert edge["source_record_id"] == person_record_id
    assert edge["target_record_id"] == entity_record_id


def test_circular_ownership_produces_two_distinct_edges() -> None:
    """A↔B cycle must emit two distinct HAS_INTEREST edges with opposing
    source/target. Dropping one leg would hide half the cycle in the
    graph."""
    fixture = load("edge-cases/10-circular-ownership")
    edges = [
        map_statement(s)
        for s in fixture.statements
        if s.get("recordType") == "relationship"
    ]
    assert len(edges) == 2, f"expected 2 edges, got {len(edges)}"

    pairs = {(e["source_record_id"], e["target_record_id"]) for e in edges}
    assert len(pairs) == 2, f"expected 2 distinct (source, target) pairs, got {pairs}"

    # The two edges should be mirror images — (A, B) and (B, A).
    sources = {e["source_record_id"] for e in edges}
    targets = {e["target_record_id"] for e in edges}
    assert sources == targets, (
        f"cycle edges aren't mirrored: sources={sources}, targets={targets}"
    )


def test_anonymous_interested_party_does_not_crash_mapper() -> None:
    """Declared-unknown UBO (inline ``unspecifiedReason`` object as
    ``interestedParty``) must not crash the mapper. The known subject
    entity must still end up as a usable node. Per FATF, a declared-
    unknown UBO is a risk signal — silently dropping the whole
    relationship or entity would erase that signal from the graph."""
    fixture = load("edge-cases/11-anonymous-person")
    mapped = [map_statement(s) for s in fixture.statements]

    # No None returns — mapper must handle the inline-object shape.
    assert all(m is not None for m in mapped), (
        "mapper returned None on declared-unknown UBO fixture"
    )

    entity_nodes = [
        m for m in mapped if m and m["type"] == "node" and "Entity" in m["labels"]
    ]
    assert entity_nodes, "no Entity node emitted despite a known subject entity"

    # The relationship edge must still exist; its source is empty string
    # (no stable record_id for an inline-unspecified interestedParty).
    edges = [m for m in mapped if m and m["type"] == "relationship"]
    assert edges, "relationship edge dropped on declared-unknown UBO"
