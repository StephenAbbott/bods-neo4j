"""Extract nodes and relationships from Neo4j for BODS conversion.

Queries the Neo4j graph database to retrieve Entity nodes, Person nodes,
and HAS_INTEREST relationships, yielding them as dictionaries ready for
BODS mapping.
"""

import logging
from typing import Generator

from ..config import Neo4jConfig
from ..utils.neo4j_helpers import neo4j_driver

logger = logging.getLogger(__name__)

# Cypher queries for extracting BODS data from Neo4j

EXTRACT_ENTITIES_QUERY = """\
MATCH (e:Entity)
RETURN e, labels(e) AS labels
ORDER BY e.recordId
"""

EXTRACT_PERSONS_QUERY = """\
MATCH (p:Person)
RETURN p, labels(p) AS labels
ORDER BY p.recordId
"""

EXTRACT_RELATIONSHIPS_QUERY = """\
MATCH (source)-[r:HAS_INTEREST]->(target)
RETURN source.recordId AS sourceRecordId,
       target.recordId AS targetRecordId,
       labels(source) AS sourceLabels,
       labels(target) AS targetLabels,
       r AS relationship
ORDER BY r.recordId
"""

EXTRACT_ALL_COUNT_QUERY = """\
MATCH (e:Entity) WITH count(e) AS entities
MATCH (p:Person) WITH entities, count(p) AS persons
MATCH ()-[r:HAS_INTEREST]->() WITH entities, persons, count(r) AS relationships
RETURN entities, persons, relationships
"""


def extract_entities(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    """Extract all Entity nodes from Neo4j.

    Yields dictionaries with node properties and labels.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_ENTITIES_QUERY, database_=neo4j_config.database
        )
        count = 0
        for record in result.records:
            node = dict(record["e"])
            node["_labels"] = record["labels"]
            yield node
            count += 1

        logger.info("Extracted %d entity nodes", count)


def extract_persons(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    """Extract all Person nodes from Neo4j.

    Yields dictionaries with node properties and labels.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_PERSONS_QUERY, database_=neo4j_config.database
        )
        count = 0
        for record in result.records:
            node = dict(record["p"])
            node["_labels"] = record["labels"]
            yield node
            count += 1

        logger.info("Extracted %d person nodes", count)


def extract_relationships(
    neo4j_config: Neo4jConfig = None,
) -> Generator[dict, None, None]:
    """Extract all HAS_INTEREST relationships from Neo4j.

    Yields dictionaries with relationship properties and endpoint record IDs.
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_RELATIONSHIPS_QUERY, database_=neo4j_config.database
        )
        count = 0
        for record in result.records:
            rel = dict(record["relationship"])
            rel["_sourceRecordId"] = record["sourceRecordId"]
            rel["_targetRecordId"] = record["targetRecordId"]
            rel["_sourceLabels"] = record["sourceLabels"]
            rel["_targetLabels"] = record["targetLabels"]
            yield rel
            count += 1

        logger.info("Extracted %d relationships", count)


def get_counts(neo4j_config: Neo4jConfig = None) -> dict:
    """Get counts of entities, persons, and relationships in the graph."""
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()

    with neo4j_driver(neo4j_config) as driver:
        result = driver.execute_query(
            EXTRACT_ALL_COUNT_QUERY, database_=neo4j_config.database
        )
        record = result.records[0]
        return {
            "entities": record["entities"],
            "persons": record["persons"],
            "relationships": record["relationships"],
        }
