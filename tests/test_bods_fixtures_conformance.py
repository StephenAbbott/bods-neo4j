"""Conformance tests against the shared bods-fixtures pack.

The pack (https://github.com/StephenAbbott/bods-fixtures) is the canonical
source of truth for BODS v0.4 shape across the adapter ecosystem. Passing
these tests means bods-neo4j's mapper agrees with the canonical envelope
that other adapters also target.

Graph-specific concerns tested here:
- circular ownership must emit two distinct relationship statements with
  mirrored party / subject record IDs.
- declared-unknown UBOs (inline ``unspecifiedReason`` objects as
  ``interestedParty``) must not crash the mapper and must leave the known
  subject entity as a usable node.

The ``bods_fixture`` parameter is auto-parametrized by the
pytest-bods-fixtures plugin over every case in the pack.
"""

from __future__ import annotations

from bods_fixtures import Fixture, load

from bods_neo4j.bods_to_neo4j.mapper import map_statement


def _has_label(node_spec, label: str) -> bool:
    return label in node_spec["labels"]


def test_every_statement_maps(bods_fixture: Fixture) -> None:
    """Every statement in every canonical fixture must map to a graph spec."""
    for stmt in bods_fixture.statements:
        graph = map_statement(stmt)
        assert graph is not None, (
            f"{bods_fixture.name}: map_statement returned None for "
            f"recordType={stmt.get('recordType')} statementId={stmt.get('statementId')}"
        )
        assert graph["statement_type"] in {"entity", "person", "relationship"}, (
            f"unexpected statement_type {graph['statement_type']!r}"
        )
        # Entity / Person statements emit a primary node; Relationship
        # statements emit typed edges directly between parties (no node
        # required) but must still emit at least one edge to surface in
        # the graph.
        if graph["statement_type"] == "relationship":
            assert graph["edges"], (
                f"{bods_fixture.name}: relationship statement "
                f"{stmt.get('statementId')} produced no edges"
            )
        else:
            assert graph["nodes"], (
                f"{bods_fixture.name}: graph for {stmt.get('statementId')} has no nodes"
            )


def test_primary_node_count_matches_fixture(bods_fixture: Fixture) -> None:
    """Entity / person statements should produce exactly one :Entity / :Person
    primary node. Relationship statements emit reified :Interest nodes only.
    """
    entity_primary = 0
    person_primary = 0
    rel_statements = 0
    for stmt in bods_fixture.statements:
        graph = map_statement(stmt)
        assert graph is not None
        if graph["statement_type"] == "entity":
            entity_primary += sum(1 for n in graph["nodes"] if _has_label(n, "Entity"))
        elif graph["statement_type"] == "person":
            person_primary += sum(1 for n in graph["nodes"] if _has_label(n, "Person"))
        else:
            rel_statements += 1

    expected_entities = len(bods_fixture.by_record_type("entity"))
    expected_persons = len(bods_fixture.by_record_type("person"))
    expected_relationships = len(bods_fixture.by_record_type("relationship"))

    assert entity_primary == expected_entities, (
        f"{bods_fixture.name}: {entity_primary} :Entity primary nodes, "
        f"expected {expected_entities}"
    )
    assert person_primary == expected_persons, (
        f"{bods_fixture.name}: {person_primary} :Person primary nodes, "
        f"expected {expected_persons}"
    )
    assert rel_statements == expected_relationships, (
        f"{bods_fixture.name}: {rel_statements} relationship-statement graphs, "
        f"expected {expected_relationships}"
    )


def test_direct_ownership_produces_entity_person_and_typed_edge() -> None:
    """The baseline fixture must produce one :Entity, one :Person, and one
    relationship graph wired (Person)-[:OWNS|CONTROLS|...]->(Entity) directly
    via a family-typed edge."""
    fixture = load("core/01-direct-ownership")
    graphs = [map_statement(s) for s in fixture.statements]

    entity_count = sum(
        1 for g in graphs if g and g["statement_type"] == "entity"
    )
    person_count = sum(
        1 for g in graphs if g and g["statement_type"] == "person"
    )
    rel_graphs = [g for g in graphs if g and g["statement_type"] == "relationship"]

    assert entity_count == 1
    assert person_count == 1
    assert len(rel_graphs) == 1

    rel = rel_graphs[0]
    assert rel["edges"], "relationship statement emitted no typed edge"
    assert rel["interested_party_record_id"], "missing interestedParty record id"
    assert rel["subject_record_id"], "missing subject record id"


def test_circular_ownership_produces_two_mirrored_statements() -> None:
    """A↔B cycle must emit two distinct relationship-statement graphs whose
    (interestedParty, subject) pairs mirror each other."""
    fixture = load("edge-cases/10-circular-ownership")
    rel_graphs = [
        map_statement(s)
        for s in fixture.statements
        if s.get("recordType") == "relationship"
    ]
    assert len(rel_graphs) == 2, f"expected 2 graphs, got {len(rel_graphs)}"

    pairs = {
        (g["interested_party_record_id"], g["subject_record_id"])
        for g in rel_graphs
    }
    assert len(pairs) == 2

    sources = {g["interested_party_record_id"] for g in rel_graphs}
    targets = {g["subject_record_id"] for g in rel_graphs}
    assert sources == targets, (
        f"cycle pairs aren't mirrored: sources={sources}, targets={targets}"
    )


def test_anonymous_interested_party_does_not_crash_mapper() -> None:
    """Declared-unknown UBO (inline ``unspecifiedReason`` object as
    ``interestedParty``) must not crash the mapper. The known subject
    entity must still end up as a usable node."""
    fixture = load("edge-cases/11-anonymous-person")
    graphs = [map_statement(s) for s in fixture.statements]
    assert all(g is not None for g in graphs), (
        "mapper returned None on declared-unknown UBO fixture"
    )

    entity_graphs = [g for g in graphs if g and g["statement_type"] == "entity"]
    assert entity_graphs, "no Entity graph emitted despite a known subject entity"

    rel_graphs = [g for g in graphs if g and g["statement_type"] == "relationship"]
    assert rel_graphs, "relationship graph dropped on declared-unknown UBO"
    # The relationship's interestedParty is None because there's no recordId
    # for the anonymous party; the subject is preserved.
    rel = rel_graphs[0]
    assert rel["subject_record_id"], "subject record id lost"
