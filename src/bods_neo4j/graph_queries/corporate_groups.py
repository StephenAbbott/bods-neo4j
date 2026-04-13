"""Corporate group analysis queries for BODS Neo4j graphs.

These queries identify and analyse corporate group structures:
- Finding all members of a corporate group
- Identifying top-level parent entities
- Measuring group depth and breadth
- Mapping multi-jurisdictional group structures
"""

import logging
from typing import Optional

from ..config import Neo4jConfig
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Find the complete corporate group from any starting entity
CORPORATE_GROUP_QUERY = """\
MATCH (start:Entity {recordId: $recordId})
CALL {
    WITH start
    MATCH (start)-[:HAS_INTEREST*0..{max_depth}]-(member)
    RETURN DISTINCT member
}
RETURN member.recordId AS recordId,
       member.name AS name,
       member.entityType AS entityType,
       member.jurisdictionCode AS jurisdictionCode,
       labels(member) AS labels
ORDER BY member.name
"""

# Find top-level parent entities (entities with no inbound HAS_INTEREST)
TOP_LEVEL_PARENTS_QUERY = """\
MATCH (e:Entity)
WHERE EXISTS {
    MATCH (e)-[:HAS_INTEREST]->()
}
AND NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(e)
}
WITH e
OPTIONAL MATCH path = (e)-[:HAS_INTEREST*]->(subsidiary:Entity)
WITH e, count(DISTINCT subsidiary) AS subsidiaryCount,
     max(length(path)) AS maxDepth
RETURN e.recordId AS recordId,
       e.name AS name,
       e.entityType AS entityType,
       e.jurisdictionCode AS jurisdictionCode,
       subsidiaryCount,
       maxDepth
ORDER BY subsidiaryCount DESC
LIMIT $limit
"""

# Analyse group by jurisdiction
GROUP_JURISDICTION_ANALYSIS_QUERY = """\
MATCH (start:Entity {recordId: $recordId})
CALL {
    WITH start
    MATCH (start)-[:HAS_INTEREST*0..{max_depth}]-(member:Entity)
    RETURN DISTINCT member
}
WITH member
RETURN member.jurisdictionCode AS jurisdiction,
       count(member) AS entityCount,
       collect(member.name) AS entityNames
ORDER BY entityCount DESC
"""

# Find group depth and structure metrics
GROUP_METRICS_QUERY = """\
MATCH (start:Entity {recordId: $recordId})
CALL {
    WITH start
    MATCH path = (start)-[:HAS_INTEREST*0..]-(member)
    RETURN DISTINCT member, length(path) AS dist
}
WITH count(DISTINCT member) AS totalMembers,
     max(dist) AS maxDepth,
     count(DISTINCT CASE WHEN member:Person THEN member END) AS personCount,
     count(DISTINCT CASE WHEN member:Entity THEN member END) AS entityCount
RETURN totalMembers, maxDepth, personCount, entityCount
"""

# Find all corporate groups in the graph with their sizes
ALL_GROUPS_QUERY = """\
MATCH (parent:Entity)
WHERE EXISTS {
    MATCH (parent)-[:HAS_INTEREST]->()
}
AND NOT EXISTS {
    MATCH ()-[:HAS_INTEREST]->(parent)
}
OPTIONAL MATCH path = (parent)-[:HAS_INTEREST*]->(sub:Entity)
WITH parent,
     count(DISTINCT sub) AS subsidiaryCount,
     max(length(path)) AS maxDepth
WHERE subsidiaryCount > 0
RETURN parent.recordId AS parentRecordId,
       parent.name AS parentName,
       parent.jurisdictionCode AS jurisdictionCode,
       subsidiaryCount,
       maxDepth
ORDER BY subsidiaryCount DESC
LIMIT $limit
"""


def find_corporate_group(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 20,
) -> list:
    """Find all members of a corporate group from any starting entity.

    Traverses HAS_INTEREST relationships bidirectionally to find the
    complete group structure.

    Args:
        record_id: recordId of any entity in the group
        neo4j_config: Neo4j connection configuration
        max_depth: Maximum traversal depth

    Returns:
        List of group member dictionaries
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = CORPORATE_GROUP_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        members = []
        for record in result.records:
            members.append({
                "recordId": record["recordId"],
                "name": record["name"],
                "entityType": record["entityType"],
                "jurisdictionCode": record["jurisdictionCode"],
                "labels": record["labels"],
            })

        logger.info("Found %d members in corporate group containing %s", len(members), record_id)
        return members


def find_top_level_parents(
    neo4j_config: Neo4jConfig = None,
    limit: int = 100,
) -> list:
    """Find top-level parent entities (roots of ownership trees).

    Returns entities that own other entities but are not owned themselves.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            TOP_LEVEL_PARENTS_QUERY,
            parameters_={"limit": limit},
            database_=neo4j_config.database,
        )

        parents = []
        for record in result.records:
            parents.append({
                "recordId": record["recordId"],
                "name": record["name"],
                "entityType": record["entityType"],
                "jurisdictionCode": record["jurisdictionCode"],
                "subsidiaryCount": record["subsidiaryCount"],
                "maxDepth": record["maxDepth"],
            })

        logger.info("Found %d top-level parent entities", len(parents))
        return parents


def analyse_group_jurisdictions(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 20,
) -> list:
    """Analyse the jurisdictional spread of a corporate group.

    Shows how many entities are in each jurisdiction within a group.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = GROUP_JURISDICTION_ANALYSIS_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        jurisdictions = []
        for record in result.records:
            jurisdictions.append({
                "jurisdiction": record["jurisdiction"],
                "entityCount": record["entityCount"],
                "entityNames": record["entityNames"],
            })

        return jurisdictions


def get_group_metrics(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
) -> dict:
    """Get summary metrics for a corporate group.

    Returns total members, max depth, person count, and entity count.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = GROUP_METRICS_QUERY.format(max_depth=50)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        if result.records:
            record = result.records[0]
            return {
                "totalMembers": record["totalMembers"],
                "maxDepth": record["maxDepth"],
                "personCount": record["personCount"],
                "entityCount": record["entityCount"],
            }
        return {"totalMembers": 0, "maxDepth": 0, "personCount": 0, "entityCount": 0}


def find_all_groups(
    neo4j_config: Neo4jConfig = None,
    limit: int = 50,
) -> list:
    """Find all corporate groups in the graph, ordered by size.

    Returns the top-level parent of each group with subsidiary counts.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            ALL_GROUPS_QUERY,
            parameters_={"limit": limit},
            database_=neo4j_config.database,
        )

        groups = []
        for record in result.records:
            groups.append({
                "parentRecordId": record["parentRecordId"],
                "parentName": record["parentName"],
                "jurisdictionCode": record["jurisdictionCode"],
                "subsidiaryCount": record["subsidiaryCount"],
                "maxDepth": record["maxDepth"],
            })

        logger.info("Found %d corporate groups", len(groups))
        return groups
