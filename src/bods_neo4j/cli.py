"""CLI interface for BODS-Neo4j converter.

Commands:
    bods-neo4j to-csv      Export BODS data to Neo4j CSV files + import scripts
    bods-neo4j to-neo4j    Load BODS data directly into Neo4j via driver
    bods-neo4j to-bods     Export Neo4j graph data back to BODS format
    bods-neo4j info        Show counts from a BODS file
    bods-neo4j graph-info  Show Neo4j graph statistics
"""

import json
import logging
import sys

import click

from .config import Neo4jConfig, ExportConfig, PublisherConfig


def _setup_logging(verbose: bool):
    """Configure logging based on verbosity."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


@click.group()
@click.version_option()
def main():
    """BODS-Neo4j: Bidirectional converter between BODS v0.4 and Neo4j.

    Convert Beneficial Ownership Data Standard (BODS) v0.4 data to and from
    Neo4j graph database format, with built-in graph analysis queries for
    UBO detection, corporate group mapping, and circular ownership detection.
    """
    pass


@main.command("to-csv")
@click.argument("bods_file", type=click.Path(exists=True))
@click.option("-o", "--output-dir", default="./neo4j_export",
              help="Output directory for CSV files and import scripts")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
def to_csv(bods_file, output_dir, verbose):
    """Export BODS data to Neo4j-importable CSV files.

    Generates entities.csv, persons.csv, relationships.csv plus
    Cypher and neo4j-admin import scripts.

    BODS_FILE is the path to a BODS JSON or JSONL file.
    """
    _setup_logging(verbose)
    from .bods_to_neo4j.csv_exporter import export_to_csv

    counts = export_to_csv(bods_file, output_dir)

    click.echo(f"\nExport complete:")
    click.echo(f"  Entities:      {counts['entities']:,}")
    click.echo(f"  Persons:       {counts['persons']:,}")
    click.echo(f"  Relationships: {counts['relationships']:,}")
    click.echo(f"  Skipped:       {counts['skipped']:,}")
    click.echo(f"\nOutput directory: {output_dir}")
    click.echo(f"  - entities.csv")
    click.echo(f"  - persons.csv")
    click.echo(f"  - relationships.csv")
    click.echo(f"  - import.cypher (for running Neo4j instance)")
    click.echo(f"  - import.sh (for neo4j-admin bulk import)")


@main.command("to-neo4j")
@click.argument("bods_file", type=click.Path(exists=True))
@click.option("--uri", envvar="NEO4J_URI", default="bolt://localhost:7687",
              help="Neo4j connection URI")
@click.option("--username", envvar="NEO4J_USERNAME", default="neo4j",
              help="Neo4j username")
@click.option("--password", envvar="NEO4J_PASSWORD", default="password",
              help="Neo4j password")
@click.option("--database", envvar="NEO4J_DATABASE", default="neo4j",
              help="Neo4j database name")
@click.option("--batch-size", default=5000, help="Records per batch transaction")
@click.option("--clear", is_flag=True, help="Clear existing data before import")
@click.option("--no-schema", is_flag=True, help="Skip constraint/index creation")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
def to_neo4j(bods_file, uri, username, password, database, batch_size, clear, no_schema, verbose):
    """Load BODS data directly into Neo4j via the Python driver.

    Best for datasets up to ~1M statements. For larger datasets,
    use 'to-csv' and the generated import scripts instead.

    BODS_FILE is the path to a BODS JSON or JSONL file.
    """
    _setup_logging(verbose)
    from .bods_to_neo4j.driver_loader import load_bods_to_neo4j

    neo4j_config = Neo4jConfig(
        uri=uri, username=username, password=password, database=database,
    )
    export_config = ExportConfig(
        batch_size=batch_size,
        clear_existing=clear,
        create_schema=not no_schema,
    )

    counts = load_bods_to_neo4j(bods_file, neo4j_config, export_config)

    click.echo(f"\nLoad complete:")
    click.echo(f"  Entities:      {counts['entities']:,}")
    click.echo(f"  Persons:       {counts['persons']:,}")
    click.echo(f"  Relationships: {counts['relationships']:,}")
    click.echo(f"  Skipped:       {counts['skipped']:,}")

    if "db_stats" in counts:
        click.echo(f"\nDatabase statistics:")
        for label, count in counts["db_stats"].get("nodes", {}).items():
            click.echo(f"  {label}: {count:,}")


@main.command("to-bods")
@click.argument("output_file", type=click.Path())
@click.option("--uri", envvar="NEO4J_URI", default="bolt://localhost:7687",
              help="Neo4j connection URI")
@click.option("--username", envvar="NEO4J_USERNAME", default="neo4j",
              help="Neo4j username")
@click.option("--password", envvar="NEO4J_PASSWORD", default="password",
              help="Neo4j password")
@click.option("--database", envvar="NEO4J_DATABASE", default="neo4j",
              help="Neo4j database name")
@click.option("--format", "output_format", type=click.Choice(["jsonl", "json"]),
              default="jsonl", help="Output format")
@click.option("--publisher-name", default="BODS Neo4j Converter",
              help="Publisher name for BODS metadata")
@click.option("--publisher-url", default=None, help="Publisher URL")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
def to_bods(output_file, uri, username, password, database, output_format,
            publisher_name, publisher_url, verbose):
    """Export Neo4j graph data to BODS v0.4 format.

    Extracts Entity nodes, Person nodes, and HAS_INTEREST relationships
    from Neo4j and converts them to valid BODS v0.4 statements.

    OUTPUT_FILE is the path for the output file (.jsonl or .json).
    """
    _setup_logging(verbose)
    from .neo4j_to_bods.writer import export_neo4j_to_bods

    neo4j_config = Neo4jConfig(
        uri=uri, username=username, password=password, database=database,
    )
    publisher_config = PublisherConfig(
        publisher_name=publisher_name,
        publisher_url=publisher_url,
    )

    counts = export_neo4j_to_bods(output_file, neo4j_config, publisher_config, output_format)

    total = counts["entities"] + counts["persons"] + counts["relationships"]
    click.echo(f"\nExport complete: {total:,} BODS statements")
    click.echo(f"  Entities:      {counts['entities']:,}")
    click.echo(f"  Persons:       {counts['persons']:,}")
    click.echo(f"  Relationships: {counts['relationships']:,}")
    click.echo(f"\nOutput: {output_file}")


@main.command("info")
@click.argument("bods_file", type=click.Path(exists=True))
def info(bods_file):
    """Show statement counts from a BODS file.

    BODS_FILE is the path to a BODS JSON or JSONL file.
    """
    from .bods_to_neo4j.reader import count_statements

    counts = count_statements(bods_file)

    click.echo(f"BODS file: {bods_file}")
    click.echo(f"  Total statements: {counts['total']:,}")
    click.echo(f"  Entity:           {counts['entity']:,}")
    click.echo(f"  Person:           {counts['person']:,}")
    click.echo(f"  Relationship:     {counts['relationship']:,}")
    if counts["other"] > 0:
        click.echo(f"  Other/unknown:    {counts['other']:,}")


@main.command("graph-info")
@click.option("--uri", envvar="NEO4J_URI", default="bolt://localhost:7687",
              help="Neo4j connection URI")
@click.option("--username", envvar="NEO4J_USERNAME", default="neo4j",
              help="Neo4j username")
@click.option("--password", envvar="NEO4J_PASSWORD", default="password",
              help="Neo4j password")
@click.option("--database", envvar="NEO4J_DATABASE", default="neo4j",
              help="Neo4j database name")
def graph_info(uri, username, password, database):
    """Show Neo4j graph statistics."""
    _setup_logging(False)
    from .utils.neo4j_helpers import neo4j_driver, get_database_stats

    neo4j_config = Neo4jConfig(
        uri=uri, username=username, password=password, database=database,
    )

    with neo4j_driver(neo4j_config) as driver:
        stats = get_database_stats(driver, database)

    click.echo(f"Neo4j graph: {uri} / {database}")
    click.echo(f"\nNodes:")
    for label, count in stats.get("nodes", {}).items():
        click.echo(f"  {label}: {count:,}")
    click.echo(f"\nRelationships:")
    for rel_type, count in stats.get("relationships", {}).items():
        click.echo(f"  {rel_type}: {count:,}")


if __name__ == "__main__":
    main()
