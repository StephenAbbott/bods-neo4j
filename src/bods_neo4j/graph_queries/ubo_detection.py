"""Ultimate Beneficial Owner (UBO) detection queries for BODS Neo4j graphs.

These queries traverse ownership chains to identify the natural persons or
entities at the top of ownership structures — the ultimate beneficial owners.

Key concepts:
- A UBO is a natural person (Person node) with significant ownership/control
  over an entity, either directly or through intermediary entities.
- Ownership percentage is calculated by multiplying shares along chains.
- Chains terminate at Person nodes (natural persons) or at Entity nodes
  with no further inbound HAS_INTEREST relationships.
"""

import logging
from typing import Optional

from ..config import Neo4jConfig
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Find all direct and indirect owners of a specific entity
FIND_OWNERS_QUERY = """\
MATCH path = (owner)-[:HAS_INTEREST*1..{max_depth}]->(target:Entity {{recordId: $recordId}})
WHERE owner:Person OR (owner:Entity AND NOT EXISTS {{
    MATCH (upstream)-[:HAS_INTEREST]->(owner)
}})
RETURN owner.recordId AS ownerRecordId,
       owner.name AS ownerName,
       labels(owner) AS ownerLabels,
       length(path) AS depth,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums,
       [r IN relationships(path) | r.shareMaximum] AS shareMaximums,
       [r IN relationships(path) | r.shareExact] AS shareExacts,
       [r IN relationships(path) | r.isBeneficialOwnership] AS boFlags,
       [r IN relationships(path) | r.interestTypes] AS interestTypes,
       [n IN nodes(path) | n.name] AS pathNames
ORDER BY depth
"""

# Find all entities owned/controlled by a specific person or entity
FIND_OWNED_ENTITIES_QUERY = """\
MATCH path = (owner {{recordId: $recordId}})-[:HAS_INTEREST*1..{max_depth}]->(target:Entity)
RETURN target.recordId AS entityRecordId,
       target.name AS entityName,
       target.jurisdictionCode AS jurisdictionCode,
       target.entityType AS entityType,
       length(path) AS depth,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums,
       [r IN relationships(path) | r.shareMaximum] AS shareMaximums,
       [r IN relationships(path) | r.shareExact] AS shareExacts,
       [n IN nodes(path) | n.name] AS pathNames
ORDER BY depth
"""

# Find all Person UBOs (natural persons at the top of ownership chains)
FIND_ALL_PERSON_UBOS_QUERY = """\
MATCH path = (person:Person)-[:HAS_INTEREST*1..{max_depth}]->(entity:Entity)
WHERE NOT EXISTS {{
    MATCH (upstream)-[:HAS_INTEREST]->(person)
}}
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

# Find entities with no identified UBO (no Person at top of chain)
FIND_ENTITIES_WITHOUT_UBOS_QUERY = """\
MATCH (e:Entity)
WHERE NOT EXISTS {
    MATCH (p:Person)-[:HAS_INTEREST*]->(e)
}
AND EXISTS {
    MATCH ()-[:HAS_INTEREST]->(e)
}
RETURN e.recordId AS recordId,
       e.name AS name,
       e.jurisdictionCode AS jurisdictionCode,
       e.entityType AS entityType
ORDER BY e.name
"""


def find_owners(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Find all direct and indirect owners of an entity.

    Args:
        record_id: The recordId of the target entity
        neo4j_config: Neo4j connection configuration
        max_depth: Maximum ownership chain depth to traverse

    Returns:
        List of owner dictionaries with ownership path details
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = FIND_OWNERS_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        owners = []
        for record in result.records:
            owner = {
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
            }
            owners.append(owner)

        logger.info("Found %d owners for entity %s", len(owners), record_id)
        return owners


def find_owned_entities(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Find all entities owned or controlled by a person or entity.

    Args:
        record_id: The recordId of the owner (person or entity)
        neo4j_config: Neo4j connection configuration
        max_depth: Maximum ownership chain depth to traverse

    Returns:
        List of owned entity dictionaries with ownership path details
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = FIND_OWNED_ENTITIES_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        entities = []
        for record in result.records:
            entity = {
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
            }
            entities.append(entity)

        logger.info("Found %d owned entities for %s", len(entities), record_id)
        return entities


def find_all_ubos(
    neo4j_config: Neo4jConfig = None,
    threshold: float = 25.0,
    max_depth: int = 10,
) -> list:
    """Find all ultimate beneficial owners across the entire graph.

    Args:
        neo4j_config: Neo4j connection configuration
        threshold: Minimum effective ownership percentage to qualify as UBO
        max_depth: Maximum ownership chain depth to traverse

    Returns:
        List of UBO dictionaries with person-to-entity relationships
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = FIND_ALL_PERSON_UBOS_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"threshold": threshold},
            database_=neo4j_config.database,
        )

        ubos = []
        for record in result.records:
            ubo = {
                "personRecordId": record["personRecordId"],
                "personName": record["personName"],
                "entityRecordId": record["entityRecordId"],
                "entityName": record["entityName"],
                "depth": record["depth"],
                "effectiveMinPct": record["effectiveMinPct"],
                "effectiveMaxPct": record["effectiveMaxPct"],
                "pathNames": record["pathNames"],
            }
            ubos.append(ubo)

        logger.info("Found %d UBO relationships (threshold: %.1f%%)", len(ubos), threshold)
        return ubos


def find_entities_without_ubos(
    neo4j_config: Neo4jConfig = None,
) -> list:
    """Find entities with incoming ownership but no natural person UBO.

    These are entities that may have opaque ownership structures.
    """
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
    """Calculate effective ownership percentage through a chain.

    Multiplies share percentages along each link in the ownership chain.
    """
    result = {}

    # Try exact values first
    exacts = [s for s in (share_exacts or []) if s is not None]
    if exacts:
        effective = 1.0
        for pct in exacts:
            effective *= float(pct) / 100.0
        result["exact"] = round(effective * 100.0, 4)
        return result

    # Otherwise use min/max ranges
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
