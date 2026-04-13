"""Load BODS data directly into Neo4j via the Python driver.

This approach is ideal for:
- Moderate datasets (up to ~1M statements)
- Incremental updates to an existing graph
- Development and testing workflows
- When you want immediate feedback and validation

For very large datasets (>1M statements), use the CSV export + neo4j-admin import instead.
"""

import logging
from typing import Union
from pathlib import Path

from ..config import Neo4jConfig, ExportConfig
from ..utils.neo4j_helpers import (
    neo4j_driver,
    create_constraints,
    create_indexes,
    create_fulltext_index,
    clear_database,
    get_database_stats,
)
from .mapper import map_statement
from .reader import read_bods_file

logger = logging.getLogger(__name__)

# Cypher queries for batch node/relationship creation
CREATE_ENTITY_QUERY = """\
UNWIND $batch AS props
MERGE (e:Entity {recordId: props.recordId})
SET e += props
"""

CREATE_PERSON_QUERY = """\
UNWIND $batch AS props
MERGE (p:Person {recordId: props.recordId})
SET p += props
"""

CREATE_RELATIONSHIP_QUERY = """\
UNWIND $batch AS rel
OPTIONAL MATCH (source:Entity {recordId: rel.sourceRecordId})
OPTIONAL MATCH (sourcePerson:Person {recordId: rel.sourceRecordId})
WITH rel, COALESCE(source, sourcePerson) AS sourceNode
MATCH (target:Entity {recordId: rel.targetRecordId})
WHERE sourceNode IS NOT NULL
CREATE (sourceNode)-[r:HAS_INTEREST]->(target)
SET r += rel.properties
"""

SET_ENTITY_SUBTYPE_LABELS = [
    ("MATCH (e:Entity) WHERE e.entityType = 'registeredEntity' SET e:RegisteredEntity", "RegisteredEntity"),
    ("MATCH (e:Entity) WHERE e.entityType = 'legalEntity' SET e:LegalEntity", "LegalEntity"),
    ("MATCH (e:Entity) WHERE e.entityType = 'arrangement' SET e:Arrangement", "Arrangement"),
    ("MATCH (e:Entity) WHERE e.entitySubtype = 'trust' SET e:Trust", "Trust"),
    ("MATCH (e:Entity) WHERE e.entitySubtype = 'nomination' SET e:Nomination", "Nomination"),
    ("MATCH (e:Entity) WHERE e.entityType = 'state' SET e:State", "State"),
    ("MATCH (e:Entity) WHERE e.entityType = 'stateBody' SET e:StateBody", "StateBody"),
    ("MATCH (e:Entity) WHERE e.entitySubtype = 'governmentDepartment' SET e:GovernmentDepartment", "GovernmentDepartment"),
    ("MATCH (e:Entity) WHERE e.entitySubtype = 'stateAgency' SET e:StateAgency", "StateAgency"),
]


def load_bods_to_neo4j(
    bods_file: Union[str, Path],
    neo4j_config: Neo4jConfig = None,
    export_config: ExportConfig = None,
) -> dict:
    """Load BODS data from a file directly into Neo4j.

    Args:
        bods_file: Path to BODS JSON or JSONL file
        neo4j_config: Neo4j connection configuration
        export_config: Export configuration options

    Returns:
        Dictionary with counts and statistics
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    if export_config is None:
        export_config = ExportConfig()

    counts = {"entities": 0, "persons": 0, "relationships": 0, "skipped": 0}

    with neo4j_driver(neo4j_config) as driver:
        database = neo4j_config.database

        # Optionally clear existing data
        if export_config.clear_existing:
            clear_database(driver, database)

        # Create schema
        if export_config.create_schema:
            create_constraints(driver, database)
            create_indexes(driver, database)

        # Collect and batch statements by type
        entity_batch = []
        person_batch = []
        relationship_batch = []
        batch_size = export_config.batch_size

        logger.info("Reading BODS statements from %s", bods_file)

        for statement in read_bods_file(bods_file):
            mapped = map_statement(statement)
            if mapped is None:
                counts["skipped"] += 1
                continue

            if mapped["type"] == "node":
                props = mapped["properties"]
                # Convert non-serialisable types
                for key, value in props.items():
                    if isinstance(value, list):
                        import json
                        props[key] = json.dumps(value)

                if "Entity" in mapped["labels"]:
                    entity_batch.append(props)
                    if len(entity_batch) >= batch_size:
                        _flush_entities(driver, entity_batch, database)
                        counts["entities"] += len(entity_batch)
                        entity_batch = []

                elif "Person" in mapped["labels"]:
                    person_batch.append(props)
                    if len(person_batch) >= batch_size:
                        _flush_persons(driver, person_batch, database)
                        counts["persons"] += len(person_batch)
                        person_batch = []

            elif mapped["type"] == "relationship":
                rel_data = {
                    "sourceRecordId": mapped.get("source_record_id", ""),
                    "targetRecordId": mapped.get("target_record_id", ""),
                    "properties": mapped["properties"],
                }
                # Convert list values in properties
                for key, value in rel_data["properties"].items():
                    if isinstance(value, list):
                        import json
                        rel_data["properties"][key] = json.dumps(value)
                relationship_batch.append(rel_data)
                if len(relationship_batch) >= batch_size:
                    _flush_relationships(driver, relationship_batch, database)
                    counts["relationships"] += len(relationship_batch)
                    relationship_batch = []

        # Flush remaining batches
        if entity_batch:
            _flush_entities(driver, entity_batch, database)
            counts["entities"] += len(entity_batch)
        if person_batch:
            _flush_persons(driver, person_batch, database)
            counts["persons"] += len(person_batch)

        # Entities and persons must be loaded before relationships
        logger.info(
            "Loaded %d entities and %d persons. Now loading %d relationships...",
            counts["entities"], counts["persons"],
            counts["relationships"] + len(relationship_batch),
        )

        if relationship_batch:
            _flush_relationships(driver, relationship_batch, database)
            counts["relationships"] += len(relationship_batch)

        # Apply subtype labels
        if export_config.use_subtype_labels:
            logger.info("Applying entity subtype labels...")
            for query, label in SET_ENTITY_SUBTYPE_LABELS:
                try:
                    driver.execute_query(query, database_=database)
                except Exception as e:
                    logger.debug("Label query for %s: %s", label, e)

        # Create full-text index
        if export_config.create_schema:
            create_fulltext_index(driver, database)

        # Get final stats
        stats = get_database_stats(driver, database)
        counts["db_stats"] = stats

    logger.info(
        "Load complete: %d entities, %d persons, %d relationships (%d skipped)",
        counts["entities"], counts["persons"], counts["relationships"], counts["skipped"],
    )

    return counts


def _flush_entities(driver, batch: list, database: str):
    """Write a batch of entity nodes to Neo4j."""
    driver.execute_query(CREATE_ENTITY_QUERY, parameters_={"batch": batch}, database_=database)
    logger.debug("Flushed %d entity nodes", len(batch))


def _flush_persons(driver, batch: list, database: str):
    """Write a batch of person nodes to Neo4j."""
    driver.execute_query(CREATE_PERSON_QUERY, parameters_={"batch": batch}, database_=database)
    logger.debug("Flushed %d person nodes", len(batch))


def _flush_relationships(driver, batch: list, database: str):
    """Write a batch of relationships to Neo4j."""
    driver.execute_query(
        CREATE_RELATIONSHIP_QUERY, parameters_={"batch": batch}, database_=database
    )
    logger.debug("Flushed %d relationships", len(batch))
