"""Corporate group analysis queries for BODS Neo4j graphs.

Typed edges sit directly between parties so traversals are single-hop
variable-length expressions over `:OWNS|:CONTROLS`.
"""

import logging

from ..config import Neo4jConfig
from ..utils.bods_schema import OWNERSHIP_CONTROL_REL_TYPES
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

_OWN_CTRL = "|".join(OWNERSHIP_CONTROL_REL_TYPES)


CORPORATE_GROUP_QUERY = f"""
MATCH (start:Entity {{{{recordId: $recordId}}}})
MATCH (start)-[:{_OWN_CTRL}*0..{{max_depth}}]-(member)
WITH DISTINCT member
RETURN member.recordId AS recordId,
       member.name AS name,
       member.entityType AS entityType,
       head([(member)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
       labels(member) AS labels
ORDER BY member.name
"""


TOP_LEVEL_PARENTS_QUERY = f"""
MATCH (e:Entity)
WHERE EXISTS {{ (e)-[:{_OWN_CTRL}]->() }}
  AND NOT EXISTS {{ ()-[:{_OWN_CTRL}]->(e) }}
WITH e
OPTIONAL MATCH path = (e)-[:{_OWN_CTRL}*1..]->(subsidiary:Entity)
WITH e, count(DISTINCT subsidiary) AS subsidiaryCount,
     max(length(path)) AS maxDepth
RETURN e.recordId AS recordId,
       e.name AS name,
       e.entityType AS entityType,
       head([(e)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
       subsidiaryCount,
       maxDepth
ORDER BY subsidiaryCount DESC
LIMIT $limit
"""


GROUP_JURISDICTION_ANALYSIS_QUERY = f"""
MATCH (start:Entity {{{{recordId: $recordId}}}})
MATCH (start)-[:{_OWN_CTRL}*0..{{max_depth}}]-(member:Entity)
WITH DISTINCT member
RETURN head([(member)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdiction,
       count(member) AS entityCount,
       collect(member.name) AS entityNames
ORDER BY entityCount DESC
"""


GROUP_METRICS_QUERY = f"""
MATCH (start:Entity {{{{recordId: $recordId}}}})
MATCH path = (start)-[:{_OWN_CTRL}*0..]-(member)
WITH count(DISTINCT member) AS totalMembers,
     max(length(path)) AS maxDepth,
     count(DISTINCT CASE WHEN member:Person THEN member END) AS personCount,
     count(DISTINCT CASE WHEN member:Entity THEN member END) AS entityCount
RETURN totalMembers, maxDepth, personCount, entityCount
"""


ALL_GROUPS_QUERY = f"""
MATCH (parent:Entity)
WHERE EXISTS {{ (parent)-[:{_OWN_CTRL}]->() }}
  AND NOT EXISTS {{ ()-[:{_OWN_CTRL}]->(parent) }}
OPTIONAL MATCH path = (parent)-[:{_OWN_CTRL}*1..]->(sub:Entity)
WITH parent,
     count(DISTINCT sub) AS subsidiaryCount,
     max(length(path)) AS maxDepth
WHERE subsidiaryCount > 0
RETURN parent.recordId AS parentRecordId,
       parent.name AS parentName,
       head([(parent)-[:REGISTERED_IN]->(c:Country) | c.code]) AS jurisdictionCode,
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
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = CORPORATE_GROUP_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"recordId": record_id},
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
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            TOP_LEVEL_PARENTS_QUERY, parameters_={"limit": limit},
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
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    query = GROUP_JURISDICTION_ANALYSIS_QUERY.format(max_depth=max_depth)
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            query, parameters_={"recordId": record_id},
            database_=neo4j_config.database,
        )
        out = []
        for record in result.records:
            out.append({
                "jurisdiction": record["jurisdiction"],
                "entityCount": record["entityCount"],
                "entityNames": record["entityNames"],
            })
        return out


def get_group_metrics(
    record_id: str,
    neo4j_config: Neo4jConfig = None,
) -> dict:
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            GROUP_METRICS_QUERY, parameters_={"recordId": record_id},
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
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            ALL_GROUPS_QUERY, parameters_={"limit": limit},
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
