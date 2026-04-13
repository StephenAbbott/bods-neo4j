"""Circular ownership detection queries for BODS Neo4j graphs.

Circular (cyclical) ownership occurs when an entity indirectly owns itself
through a chain of intermediary entities. This is a key indicator of
complex ownership structures that may be used to obscure beneficial ownership.

Examples:
- Company A owns Company B, Company B owns Company A
- Company A -> Company B -> Company C -> Company A
"""

import logging

from ..config import Neo4jConfig
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Detect all circular ownership chains
FIND_CYCLES_QUERY = """\
MATCH path = (e:Entity)-[:HAS_INTEREST*2..{max_depth}]->(e)
WITH e, path, length(path) AS cycleLength
RETURN DISTINCT e.recordId AS entityRecordId,
       e.name AS entityName,
       e.jurisdictionCode AS jurisdictionCode,
       cycleLength,
       [n IN nodes(path) | n.name] AS cycleNames,
       [n IN nodes(path) | n.recordId] AS cycleRecordIds
ORDER BY cycleLength
"""

# Check if a specific entity is part of a circular ownership structure
CHECK_ENTITY_CYCLE_QUERY = """\
MATCH path = (e:Entity {recordId: $recordId})-[:HAS_INTEREST*2..{max_depth}]->(e)
RETURN length(path) AS cycleLength,
       [n IN nodes(path) | n.name] AS cycleNames,
       [n IN nodes(path) | n.recordId] AS cycleRecordIds,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums
ORDER BY cycleLength
"""

# Find entities involved in mutual ownership (A owns B and B owns A)
MUTUAL_OWNERSHIP_QUERY = """\
MATCH (a:Entity)-[r1:HAS_INTEREST]->(b:Entity)-[r2:HAS_INTEREST]->(a)
WHERE id(a) < id(b)
RETURN a.recordId AS entityA_recordId,
       a.name AS entityA_name,
       b.recordId AS entityB_recordId,
       b.name AS entityB_name,
       r1.shareMinimum AS a_owns_b_min,
       r1.shareMaximum AS a_owns_b_max,
       r2.shareMinimum AS b_owns_a_min,
       r2.shareMaximum AS b_owns_a_max
ORDER BY a.name
"""

# Summary statistics on circular ownership
CYCLE_STATS_QUERY = """\
MATCH path = (e:Entity)-[:HAS_INTEREST*2..{max_depth}]->(e)
WITH e, length(path) AS cycleLength
RETURN count(DISTINCT e) AS entitiesInCycles,
       min(cycleLength) AS shortestCycle,
       max(cycleLength) AS longestCycle,
       avg(cycleLength) AS avgCycleLength,
       count(path) AS totalCyclePaths
"""


def find_circular_ownership(
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Find all circular ownership structures in the graph.

    Args:
        neo4j_config: Neo4j connection configuration
        max_depth: Maximum cycle length to search for

    Returns:
        List of cycle dictionaries with entity details
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = FIND_CYCLES_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(query, database_=neo4j_config.database)

        cycles = []
        for record in result.records:
            cycles.append({
                "entityRecordId": record["entityRecordId"],
                "entityName": record["entityName"],
                "jurisdictionCode": record["jurisdictionCode"],
                "cycleLength": record["cycleLength"],
                "cycleNames": record["cycleNames"],
                "cycleRecordIds": record["cycleRecordIds"],
            })

        logger.info("Found %d circular ownership structures", len(cycles))
        return cycles


def check_entity_for_cycles(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> list:
    """Check if a specific entity is part of any circular ownership structure.

    Args:
        record_id: recordId of the entity to check
        neo4j_config: Neo4j connection configuration
        max_depth: Maximum cycle length to search for

    Returns:
        List of cycles involving this entity (empty if none)
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = CHECK_ENTITY_CYCLE_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query,
            parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )

        cycles = []
        for record in result.records:
            cycles.append({
                "cycleLength": record["cycleLength"],
                "cycleNames": record["cycleNames"],
                "cycleRecordIds": record["cycleRecordIds"],
            })

        if cycles:
            logger.warning("Entity %s is part of %d circular ownership structures",
                           record_id, len(cycles))
        return cycles


def find_mutual_ownership(
    neo4j_config: Neo4jConfig = None,
) -> list:
    """Find pairs of entities that own each other (mutual/reciprocal ownership).

    This is the simplest form of circular ownership (cycle of length 2).
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            MUTUAL_OWNERSHIP_QUERY, database_=neo4j_config.database
        )

        pairs = []
        for record in result.records:
            pairs.append({
                "entityA": {
                    "recordId": record["entityA_recordId"],
                    "name": record["entityA_name"],
                    "ownsMin": record["a_owns_b_min"],
                    "ownsMax": record["a_owns_b_max"],
                },
                "entityB": {
                    "recordId": record["entityB_recordId"],
                    "name": record["entityB_name"],
                    "ownsMin": record["b_owns_a_min"],
                    "ownsMax": record["b_owns_a_max"],
                },
            })

        logger.info("Found %d mutual ownership pairs", len(pairs))
        return pairs


def get_cycle_statistics(
    neo4j_config: Neo4jConfig = None,
    max_depth: int = 10,
) -> dict:
    """Get summary statistics on circular ownership in the graph."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    query = CYCLE_STATS_QUERY.format(max_depth=max_depth)

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(query, database_=neo4j_config.database)

        if result.records:
            record = result.records[0]
            return {
                "entitiesInCycles": record["entitiesInCycles"],
                "shortestCycle": record["shortestCycle"],
                "longestCycle": record["longestCycle"],
                "avgCycleLength": record["avgCycleLength"],
                "totalCyclePaths": record["totalCyclePaths"],
            }
        return {
            "entitiesInCycles": 0,
            "shortestCycle": 0,
            "longestCycle": 0,
            "avgCycleLength": 0,
            "totalCyclePaths": 0,
        }
