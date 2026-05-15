"""Extract statement-shaped data from a graph-native BODS Neo4j database.

Returns a structured envelope per BODS statement — Entity, Person, or
Relationship — with all child-node aggregates already gathered via Cypher
pattern comprehensions. The reverse mapper at ``neo4j_to_bods.mapper`` turns
each envelope into a valid BODS v0.4 statement.
"""

import logging
from typing import Generator

from ..config import Neo4jConfig
from ..utils.bods_schema import FAMILY_REL_TYPES
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Pre-computed UNION of family relationship types for use in pattern comprehensions.
_FAMILY_UNION = "|".join(FAMILY_REL_TYPES)


EXTRACT_ENTITIES_QUERY = f"""
MATCH (e:Entity)
RETURN
  properties(e) AS node,
  labels(e) AS labels,
  head([(e)-[:REGISTERED_IN]->(c:Country) | properties(c)]) AS jurisdiction,
  [(e)-[hi:HAS_IDENTIFIER]->(ident:Identifier) |
      {{ident: properties(ident),
        ordinal: hi.ordinal,
        isPrimary: hi.isPrimary,
        nullFields: hi.nullFields}}
  ] AS identifiers,
  [(e)-[ha:HAS_ADDRESS]->(addr:Address) |
      {{addr: properties(addr),
        type: ha.type,
        ordinal: ha.ordinal,
        country: head([(addr)-[:LOCATED_IN]->(c:Country) | properties(c)])}}
  ] AS addresses
ORDER BY e.recordId
"""

EXTRACT_PERSONS_QUERY = f"""
MATCH (p:Person)
RETURN
  properties(p) AS node,
  labels(p) AS labels,
  [(p)-[hi:HAS_IDENTIFIER]->(ident:Identifier) |
      {{ident: properties(ident),
        ordinal: hi.ordinal,
        isPrimary: hi.isPrimary,
        nullFields: hi.nullFields}}
  ] AS identifiers,
  [(p)-[ha:HAS_ADDRESS]->(addr:Address) |
      {{addr: properties(addr),
        type: ha.type,
        ordinal: ha.ordinal,
        country: head([(addr)-[:LOCATED_IN]->(c:Country) | properties(c)])}}
  ] AS addresses,
  head([(p)-[:BORN_IN]->(addr:Address) |
        {{addr: properties(addr),
          country: head([(addr)-[:LOCATED_IN]->(c:Country) | properties(c)])}}
  ]) AS placeOfBirth
ORDER BY p.recordId
"""

# Relationship statements live as typed edges; one BODS statement can fan
# out into N edges (one per interests[] entry) that all share the same
# `statementId`. We group by statementId and collect properties from each
# edge plus its endpoints, then yield one envelope per BODS statement.
EXTRACT_RELATIONSHIPS_QUERY = f"""
MATCH (party)-[r:{_FAMILY_UNION}]->(subject)
WITH r.statementId AS statementId, r, party, subject
ORDER BY r.interestIndex
WITH statementId,
     collect({{
        props: properties(r),
        relType: type(r),
        partyRecordId: party.recordId,
        partyLabels: labels(party),
        partyUnspecifiedUid: CASE WHEN party:UnspecifiedParty THEN party.uid ELSE null END,
        partyUnspecifiedProps: CASE WHEN party:UnspecifiedParty THEN properties(party) ELSE null END,
        subjectRecordId: subject.recordId,
        subjectLabels: labels(subject),
        subjectUnspecifiedUid: CASE WHEN subject:UnspecifiedParty THEN subject.uid ELSE null END,
        subjectUnspecifiedProps: CASE WHEN subject:UnspecifiedParty THEN properties(subject) ELSE null END
     }}) AS edges
RETURN statementId, edges
ORDER BY statementId
"""

EXTRACT_COUNTS_QUERY = f"""
MATCH (e:Entity) WITH count(e) AS entities
MATCH (p:Person) WITH entities, count(p) AS persons
OPTIONAL MATCH ()-[r:{_FAMILY_UNION}]->()
WITH entities, persons, count(DISTINCT r.statementId) AS relationships
RETURN entities, persons, relationships
"""


def extract_entity_statements(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_ENTITIES_QUERY, database_=neo4j_config.database,
        )
        count = 0
        for record in result.records:
            yield {
                "node": dict(record["node"]),
                "labels": list(record["labels"] or []),
                "jurisdiction": dict(record["jurisdiction"]) if record["jurisdiction"] else None,
                "identifiers": [dict(x) for x in (record["identifiers"] or [])],
                "addresses": [dict(x) for x in (record["addresses"] or [])],
            }
            count += 1
        logger.info("Extracted %d entity statements", count)


def extract_person_statements(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_PERSONS_QUERY, database_=neo4j_config.database,
        )
        count = 0
        for record in result.records:
            yield {
                "node": dict(record["node"]),
                "labels": list(record["labels"] or []),
                "identifiers": [dict(x) for x in (record["identifiers"] or [])],
                "addresses": [dict(x) for x in (record["addresses"] or [])],
                "place_of_birth": dict(record["placeOfBirth"]) if record["placeOfBirth"] else None,
            }
            count += 1
        logger.info("Extracted %d person statements", count)


def extract_relationship_statements(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_RELATIONSHIPS_QUERY, database_=neo4j_config.database,
        )
        count = 0
        for record in result.records:
            edges = [dict(e) for e in (record["edges"] or [])]
            first = edges[0] if edges else {}
            yield {
                "statement_id": record["statementId"],
                "edges": [dict(e["props"]) for e in edges],
                "edge_rel_types": [e["relType"] for e in edges],
                "interested_party_record_id": first.get("partyRecordId"),
                "interested_party_labels": list(first.get("partyLabels") or []),
                "interested_party_unspecified": (
                    dict(first["partyUnspecifiedProps"])
                    if first.get("partyUnspecifiedProps") else None
                ),
                "subject_record_id": first.get("subjectRecordId"),
                "subject_labels": list(first.get("subjectLabels") or []),
                "subject_unspecified": (
                    dict(first["subjectUnspecifiedProps"])
                    if first.get("subjectUnspecifiedProps") else None
                ),
            }
            count += 1
        logger.info("Extracted %d relationship statements", count)


def get_counts(neo4j_config: Neo4jConfig = None) -> dict:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_COUNTS_QUERY, database_=neo4j_config.database,
        )
        record = result.records[0]
        return {
            "entities": record["entities"],
            "persons": record["persons"],
            "relationships": record["relationships"],
        }


# ---------------------------------------------------------------------------
# Back-compat exports.
# ---------------------------------------------------------------------------

extract_entities = extract_entity_statements
extract_persons = extract_person_statements
extract_relationships = extract_relationship_statements
