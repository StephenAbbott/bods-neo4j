"""Circular ownership detection queries for BODS Neo4j graphs.

With typed edges directly between parties, cycles traverse a single edge
type set:

    MATCH path = (e:Entity)-[:OWNS|CONTROLS*2..10]->(e)
"""

import logging

from ..config import Neo4jConfig
from ..utils.bods_schema import OWNERSHIP_CONTROL_REL_TYPES
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

_OWN_CTRL = "|".join(OWNERSHIP_CONTROL_REL_TYPES)


FIND_CYCLES_QUERY = f"""
MATCH path = (e:Entity)-[:{_OWN_CTRL}*2..{{max_depth}}]->(e)
WITH e, path, length(path) AS cycleLength
RETURN DISTINCT e.recordId AS entityRecordId,
       e.name AS entityName,
       head([(e)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
       cycleLength,
       [n IN nodes(path) | n.name] AS cycleNames,
       [n IN nodes(path) | n.recordId] AS cycleRecordIds
ORDER BY cycleLength
"""


CHECK_ENTITY_CYCLE_QUERY = f"""
MATCH path = (e:Entity {{{{recordId: $recordId}}}})-[:{_OWN_CTRL}*2..{{max_depth}}]->(e)
RETURN length(path) AS cycleLength,
       [n IN nodes(path) | n.name] AS cycleNames,
       [n IN nodes(path) | n.recordId] AS cycleRecordIds,
       [r IN relationships(path) | r.shareMinimum] AS shareMinimums
ORDER BY cycleLength
"""


MUTUAL_OWNERSHIP_QUERY = f"""
MATCH (a:Entity)-[r1:{_OWN_CTRL}]->(b:Entity)-[r2:{_OWN_CTRL}]->(a)
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


CYCLE_STATS_QUERY = f"""
MATCH path = (e:Entity)-[:{_OWN_CTRL}*2..{{max_depth}}]->(e)
WITH e, length(path) AS cycleLength, path
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
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = CHECK_ENTITY_CYCLE_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"recordId": record_id},
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
            logger.warning(
                "Entity %s is part of %d circular ownership structures",
                record_id, len(cycles),
            )
        return cycles


def find_mutual_ownership(neo4j_config: Neo4jConfig = None) -> list:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            MUTUAL_OWNERSHIP_QUERY, database_=neo4j_config.database,
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
