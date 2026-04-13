"""Neo4j connection and batch operation helpers."""

import logging
from contextlib import contextmanager
from typing import Generator, Optional

from neo4j import GraphDatabase

from ..config import Neo4jConfig

logger = logging.getLogger(__name__)


@contextmanager
def neo4j_driver(config: Neo4jConfig) -> Generator:
    """Context manager for Neo4j driver connection.

    Usage:
        with neo4j_driver(config) as driver:
            driver.execute_query("MATCH (n) RETURN count(n)")
    """
    driver = GraphDatabase.driver(
        config.uri,
        auth=(config.username, config.password),
    )
    try:
        driver.verify_connectivity()
        logger.info("Connected to Neo4j at %s", config.uri)
        yield driver
    finally:
        driver.close()
        logger.info("Disconnected from Neo4j")


def execute_query(driver, query: str, parameters: Optional[dict] = None, database: str = "neo4j"):
    """Execute a Cypher query and return results."""
    result = driver.execute_query(query, parameters_=parameters or {}, database_=database)
    return result


def batch_execute(
    driver,
    query: str,
    data: list,
    batch_size: int = 5000,
    database: str = "neo4j",
) -> int:
    """Execute a parameterised Cypher query in batches.

    Args:
        driver: Neo4j driver instance
        query: Cypher query with $batch parameter (list of dicts)
        data: List of parameter dictionaries
        batch_size: Number of records per transaction
        database: Target database name

    Returns:
        Total number of records processed
    """
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i : i + batch_size]
        driver.execute_query(query, parameters_={"batch": batch}, database_=database)
        total += len(batch)
        if total % (batch_size * 10) == 0:
            logger.info("Processed %d / %d records", total, len(data))

    logger.info("Batch complete: %d records processed", total)
    return total


def create_constraints(driver, database: str = "neo4j"):
    """Create uniqueness constraints for BODS nodes.

    These constraints also create indexes for fast lookups.
    """
    constraints = [
        (
            "constraint_entity_record_id",
            "CREATE CONSTRAINT constraint_entity_record_id IF NOT EXISTS "
            "FOR (e:Entity) REQUIRE e.recordId IS UNIQUE",
        ),
        (
            "constraint_person_record_id",
            "CREATE CONSTRAINT constraint_person_record_id IF NOT EXISTS "
            "FOR (p:Person) REQUIRE p.recordId IS UNIQUE",
        ),
        (
            "constraint_entity_statement_id",
            "CREATE CONSTRAINT constraint_entity_statement_id IF NOT EXISTS "
            "FOR (e:Entity) REQUIRE e.statementId IS UNIQUE",
        ),
        (
            "constraint_person_statement_id",
            "CREATE CONSTRAINT constraint_person_statement_id IF NOT EXISTS "
            "FOR (p:Person) REQUIRE p.statementId IS UNIQUE",
        ),
    ]

    for name, query in constraints:
        try:
            driver.execute_query(query, database_=database)
            logger.info("Created constraint: %s", name)
        except Exception as e:
            logger.debug("Constraint %s may already exist: %s", name, e)


def create_indexes(driver, database: str = "neo4j"):
    """Create additional indexes for common query patterns."""
    indexes = [
        (
            "idx_entity_name",
            "CREATE INDEX idx_entity_name IF NOT EXISTS FOR (e:Entity) ON (e.name)",
        ),
        (
            "idx_person_name",
            "CREATE INDEX idx_person_name IF NOT EXISTS FOR (p:Person) ON (p.name)",
        ),
        (
            "idx_entity_jurisdiction",
            "CREATE INDEX idx_entity_jurisdiction IF NOT EXISTS "
            "FOR (e:Entity) ON (e.jurisdictionCode)",
        ),
        (
            "idx_entity_type",
            "CREATE INDEX idx_entity_type IF NOT EXISTS FOR (e:Entity) ON (e.entityType)",
        ),
    ]

    for name, query in indexes:
        try:
            driver.execute_query(query, database_=database)
            logger.info("Created index: %s", name)
        except Exception as e:
            logger.debug("Index %s may already exist: %s", name, e)


def create_fulltext_index(driver, database: str = "neo4j"):
    """Create a full-text search index across entity and person names."""
    query = (
        'CREATE FULLTEXT INDEX bods_names IF NOT EXISTS '
        'FOR (n:Entity|Person) ON EACH [n.name]'
    )
    try:
        driver.execute_query(query, database_=database)
        logger.info("Created full-text index: bods_names")
    except Exception as e:
        logger.debug("Full-text index may already exist: %s", e)


def clear_database(driver, database: str = "neo4j"):
    """Remove all nodes and relationships from the database.

    WARNING: This is destructive. Use with caution.
    """
    logger.warning("Clearing all data from database '%s'", database)
    driver.execute_query(
        "CALL { MATCH (n) DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS",
        database_=database,
    )
    logger.info("Database cleared")


def get_database_stats(driver, database: str = "neo4j") -> dict:
    """Get counts of nodes and relationships in the database."""
    node_result = driver.execute_query(
        "MATCH (n) RETURN labels(n) AS labels, count(n) AS count",
        database_=database,
    )
    rel_result = driver.execute_query(
        "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count",
        database_=database,
    )

    stats = {
        "nodes": {str(r["labels"]): r["count"] for r in node_result.records},
        "relationships": {r["type"]: r["count"] for r in rel_result.records},
    }
    return stats
