"""Configuration for BODS-Neo4j converter."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Neo4jConfig:
    """Neo4j connection configuration."""

    uri: str = "bolt://localhost:7687"
    username: str = "neo4j"
    password: str = "password"
    database: str = "neo4j"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        """Create config from environment variables."""
        import os

        return cls(
            uri=os.environ.get("NEO4J_URI", cls.uri),
            username=os.environ.get("NEO4J_USERNAME", cls.username),
            password=os.environ.get("NEO4J_PASSWORD", cls.password),
            database=os.environ.get("NEO4J_DATABASE", cls.database),
        )


@dataclass
class PublisherConfig:
    """BODS publisher metadata for Neo4j-to-BODS export."""

    publisher_name: str = "BODS Neo4j Converter"
    publisher_url: Optional[str] = None
    license_url: Optional[str] = None
    bods_version: str = "0.4"
    source_type: str = "thirdParty"
    source_description: str = "Extracted from Neo4j graph database"


@dataclass
class ExportConfig:
    """Configuration for BODS-to-Neo4j export."""

    # Output mode: "csv" for CSV files + import scripts, "driver" for direct Neo4j loading
    mode: str = "csv"
    # Output directory for CSV export
    output_dir: str = "./neo4j_export"
    # Batch size for driver-based loading
    batch_size: int = 5000
    # Whether to create constraints and indexes
    create_schema: bool = True
    # Whether to clear existing data before import
    clear_existing: bool = False
    # Node labels configuration
    entity_labels: list = field(default_factory=lambda: ["Entity"])
    person_labels: list = field(default_factory=lambda: ["Person"])
    # Relationship type
    interest_rel_type: str = "HAS_INTEREST"
    # Whether to add entity subtype as additional label
    use_subtype_labels: bool = True
