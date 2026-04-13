"""Write BODS statements extracted from Neo4j to JSON/JSONL files."""

import json
import logging
from pathlib import Path
from typing import Union

from ..config import Neo4jConfig, PublisherConfig
from .extractor import extract_entities, extract_persons, extract_relationships
from .mapper import map_entity_node, map_person_node, map_relationship

logger = logging.getLogger(__name__)


def export_neo4j_to_bods(
    output_file: Union[str, Path],
    neo4j_config: Neo4jConfig = None,
    publisher_config: PublisherConfig = None,
    output_format: str = "jsonl",
) -> dict:
    """Export Neo4j graph data to a BODS v0.4 file.

    Args:
        output_file: Path for the output file (.json or .jsonl)
        neo4j_config: Neo4j connection configuration
        publisher_config: BODS publisher metadata
        output_format: "jsonl" (streaming, recommended) or "json" (array)

    Returns:
        Dictionary with counts of exported statements
    """
    if neo4j_config is None:
        neo4j_config = Neo4jConfig.from_env()
    if publisher_config is None:
        publisher_config = PublisherConfig()

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    counts = {"entities": 0, "persons": 0, "relationships": 0}

    if output_format == "jsonl":
        counts = _write_jsonl(output_file, neo4j_config, publisher_config)
    elif output_format == "json":
        counts = _write_json(output_file, neo4j_config, publisher_config)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

    total = counts["entities"] + counts["persons"] + counts["relationships"]
    logger.info(
        "Exported %d BODS statements to %s (%d entities, %d persons, %d relationships)",
        total, output_file, counts["entities"], counts["persons"], counts["relationships"],
    )

    return counts


def _write_jsonl(
    output_file: Path,
    neo4j_config: Neo4jConfig,
    publisher_config: PublisherConfig,
) -> dict:
    """Write BODS statements as JSONL (one statement per line)."""
    counts = {"entities": 0, "persons": 0, "relationships": 0}

    with open(output_file, "w", encoding="utf-8") as f:
        # Write entity statements first (referenced by relationships)
        for node in extract_entities(neo4j_config):
            statement = map_entity_node(node, publisher_config)
            f.write(json.dumps(statement, ensure_ascii=False) + "\n")
            counts["entities"] += 1

        # Write person statements
        for node in extract_persons(neo4j_config):
            statement = map_person_node(node, publisher_config)
            f.write(json.dumps(statement, ensure_ascii=False) + "\n")
            counts["persons"] += 1

        # Write relationship statements last
        for rel in extract_relationships(neo4j_config):
            statement = map_relationship(rel, publisher_config)
            f.write(json.dumps(statement, ensure_ascii=False) + "\n")
            counts["relationships"] += 1

    return counts


def _write_json(
    output_file: Path,
    neo4j_config: Neo4jConfig,
    publisher_config: PublisherConfig,
) -> dict:
    """Write BODS statements as a JSON array."""
    counts = {"entities": 0, "persons": 0, "relationships": 0}
    statements = []

    for node in extract_entities(neo4j_config):
        statements.append(map_entity_node(node, publisher_config))
        counts["entities"] += 1

    for node in extract_persons(neo4j_config):
        statements.append(map_person_node(node, publisher_config))
        counts["persons"] += 1

    for rel in extract_relationships(neo4j_config):
        statements.append(map_relationship(rel, publisher_config))
        counts["relationships"] += 1

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(statements, f, ensure_ascii=False, indent=2)

    return counts
