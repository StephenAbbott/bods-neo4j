"""Ultimate Beneficial Owner (UBO) detection queries for BODS Neo4j graphs.

The graph carries BODS interests directly on typed relationships
(`:OWNS|:CONTROLS|:MANAGES|:IS_PARTY_TO|:HAS_OTHER_INTEREST`), so ownership
chains are single-hop variable-length traversals:

    MATCH path = (owner)-[:OWNS|CONTROLS*1..10]->(target:Entity)

For UBO traversal we restrict the rel-type union to the
``OWNERSHIP_CONTROL_REL_TYPES`` constant (OWNS + CONTROLS) — management /
arrangement edges aren't typically counted as beneficial-ownership chains.
"""

import logging

from ..config import Neo4jConfig
from ..utils.bods_schema import OWNERSHIP_CONTROL_REL_TYPES
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Cypher pipe-union of ownership/control rel types: "OWNS|CONTROLS"
_OWN_CTRL = "|".join(OWNERSHIP_CONTROL_REL_TYPES)


FIND_OWNERS_QUERY = f"""
MATCH path = (owner)-[:{_OWN_CTRL}*1..{{max_depth}}]->(target:Entity {{{{recordId: $recordId}}}})
WHERE owner:Person OR
      (owner:Entity AND NOT EXISTS {{{{ ()-[:{_OWN_CTRL}]->(owner) }}}})
RETURN owner.recordId AS ownerRecordId,
       owner.name AS ownerName,
       labels(owner) AS ownerLabels,
       length(path) AS depth,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums,
       [r IN relationships(path) | r.shareMaximum] AS shareMaximums,
       [r IN relationships(path) | r.shareExact] AS shareExacts,
       [r IN relationships(path) | r.beneficialOwnershipOrControl] AS boFlags,
       [r IN relationships(path) | r.bodsInterestType] AS interestTypes,
       [n IN nodes(path) | n.name] AS pathNames
ORDER BY depth
"""

FIND_OWNED_ENTITIES_QUERY = f"""
MATCH path = (owner {{{{recordId: $recordId}}}})-[:{_OWN_CTRL}*1..{{max_depth}}]->(target:Entity)
RETURN target.recordId AS entityRecordId,
       target.name AS entityName,
       head([(target)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
       target.entityType AS entityType,
       length(path) AS depth,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums,
       [r IN relationships(path) | r.shareMaximum] AS shareMaximums,
       [r IN relationships(path) | r.shareExact] AS shareExacts,
       [n IN nodes(path) | n.name] AS pathNames
ORDER BY depth
"""

FIND_ALL_PERSON_UBOS_QUERY = f"""
MATCH path = (person:Person)-[:{_OWN_CTRL}*1..{{max_depth}}]->(entity:Entity)
WHERE NOT EXISTS {{{{ ()-[:{_OWN_CTRL}]->(person) }}}}
WITH person, entity, path,
     reduce(minPct = 1.0, r IN relationships(path) |
         CASE WHEN r.shareMinimum IS NOT NULL
              THEN minPct * (toFloat(r.shareMinimum) / 100.0)
              ELSE minPct END) * 100.0 AS effectiveMinPct,
     reduce(maxPct = 1.0, r IN relationships(path) |
         CASE WHEN r.shareMaximum IS NOT NULL
              THEN maxPct * (toFloat(r.shareMaximum) / 100.0)
              ELSE maxPct END) * 100.0 AS effectiveMaxPct
WHERE effectiveMinPct >= $threshold
RETURN person.recordId AS personRecordId,
       person.name AS personName,
       entity.recordId AS entityRecordId,
       entity.name AS entityName,
       length(path) AS depth,
       effectiveMinPct,
       effectiveMaxPct,
       [n IN nodes(path) | n.name] AS pathNames
ORDER BY effectiveMinPct DESC
"""

FIND_ENTITIES_WITHOUT_UBOS_QUERY = f"""
MATCH (e:Entity)
WHERE EXISTS {{ ()-[:{_OWN_CTRL}]->(e) }}
  AND NOT EXISTS {{ (:Person)-[:{_OWN_CTRL}*1..20]->(e) }}
RETURN e.recordId AS recordId,
       e.name AS name,
       head([(e)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
       e.entityType AS entityType
ORDER BY e.name
"""


def find_owners(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Find all direct and indirect owners of an entity."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = FIND_OWNERS_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )
        owners = []
        for record in result.records:
            owners.append({
                "ownerRecordId": record["ownerRecordId"],
                "ownerName": record["ownerName"],
                "ownerType": "person" if "Person" in record["ownerLabels"] else "entity",
                "depth": record["depth"],
                "pathNames": record["pathNames"],
                "effectiveOwnership": _calculate_effective_ownership(
                    record["shareMinimums"],
                    record["shareMaximums"],
                    record["shareExacts"],
                ),
            })
        logger.info("Found %d owners for entity %s", len(owners), record_id)
        return owners


def find_owned_entities(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Find all entities owned or controlled by a person or entity."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = FIND_OWNED_ENTITIES_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )
        entities = []
        for record in result.records:
            entities.append({
                "entityRecordId": record["entityRecordId"],
                "entityName": record["entityName"],
                "jurisdictionCode": record["jurisdictionCode"],
                "entityType": record["entityType"],
                "depth": record["depth"],
                "pathNames": record["pathNames"],
                "effectiveOwnership": _calculate_effective_ownership(
                    record["shareMinimums"],
                    record["shareMaximums"],
                    record["shareExacts"],
                ),
            })
        logger.info("Found %d owned entities for %s", len(entities), record_id)
        return entities


def find_all_ubos(
    neo4j_config: Neo4jConfig = None,
    threshold: float = 25.0,
    max_depth: int = 10,
) -> list:
    """Find all ultimate beneficial owners across the graph."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = FIND_ALL_PERSON_UBOS_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"threshold": threshold},
            database_=neo4j_config.database,
        )
        ubos = []
        for record in result.records:
            ubos.append({
                "personRecordId": record["personRecordId"],
                "personName": record["personName"],
                "entityRecordId": record["entityRecordId"],
                "entityName": record["entityName"],
                "depth": record["depth"],
                "effectiveMinPct": record["effectiveMinPct"],
                "effectiveMaxPct": record["effectiveMaxPct"],
                "pathNames": record["pathNames"],
            })
        logger.info("Found %d UBO relationships (threshold: %.1f%%)", len(ubos), threshold)
        return ubos


def find_entities_without_ubos(
    neo4j_config: Neo4jConfig = None,
) -> list:
    """Find entities with incoming ownership but no natural-person UBO."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            FIND_ENTITIES_WITHOUT_UBOS_QUERY,
            database_=neo4j_config.database,
        )
        entities = []
        for record in result.records:
            entities.append({
                "recordId": record["recordId"],
                "name": record["name"],
                "jurisdictionCode": record["jurisdictionCode"],
                "entityType": record["entityType"],
            })
        logger.info("Found %d entities without identified UBOs", len(entities))
        return entities


def _calculate_effective_ownership(
    share_minimums: list,
    share_maximums: list,
    share_exacts: list,
) -> dict:
    """Calculate effective ownership percentage through a chain."""
    result = {}

    exacts = [s for s in (share_exacts or []) if s is not None]
    if exacts:
        effective = 1.0
        for pct in exacts:
            effective *= float(pct) / 100.0
        result["exact"] = round(effective * 100.0, 4)
        return result

    mins = [s for s in (share_minimums or []) if s is not None]
    maxs = [s for s in (share_maximums or []) if s is not None]

    if mins:
        effective_min = 1.0
        for pct in mins:
            effective_min *= float(pct) / 100.0
        result["minimum"] = round(effective_min * 100.0, 4)

    if maxs:
        effective_max = 1.0
        for pct in maxs:
            effective_max *= float(pct) / 100.0
        result["maximum"] = round(effective_max * 100.0, 4)

    return result
