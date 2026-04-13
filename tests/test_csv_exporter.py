"""Tests for CSV exporter."""

import csv
import os
import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.csv_exporter import export_to_csv

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


class TestCsvExport:
    """Tests for CSV export functionality."""

    @pytest.fixture
    def export_dir(self):
        """Create a temporary directory for export output."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir

    def test_export_creates_files(self, export_dir):
        """Export creates all expected output files."""
        export_to_csv(SAMPLE_FILE, export_dir)

        expected_files = [
            "entities.csv",
            "persons.csv",
            "relationships.csv",
            "import.cypher",
            "import.sh",
            "post_import_indexes.cypher",
        ]
        for filename in expected_files:
            assert (Path(export_dir) / filename).exists(), f"Missing: {filename}"

    def test_export_counts(self, export_dir):
        """Export returns correct counts."""
        counts = export_to_csv(SAMPLE_FILE, export_dir)

        assert counts["entities"] == 3
        assert counts["persons"] == 2
        assert counts["relationships"] == 3
        assert counts["skipped"] == 0

    def test_entities_csv_content(self, export_dir):
        """Entities CSV contains correct data."""
        export_to_csv(SAMPLE_FILE, export_dir)

        with open(Path(export_dir) / "entities.csv", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

        # Check first entity (Alpha Corp)
        alpha = next(r for r in rows if r["name"] == "Alpha Corp")
        assert alpha["recordId"] == "rec-entity-alpha"
        assert alpha["entityType"] == "registeredEntity"
        assert alpha["jurisdictionCode"] == "GB"
        assert alpha["foundingDate"] == "2020-01-01"

    def test_persons_csv_content(self, export_dir):
        """Persons CSV contains correct data."""
        export_to_csv(SAMPLE_FILE, export_dir)

        with open(Path(export_dir) / "persons.csv", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2

        alice = next(r for r in rows if r["name"] == "Alice Johnson")
        assert alice["recordId"] == "rec-person-alice"
        assert alice["personType"] == "knownPerson"
        assert alice["familyName"] == "Johnson"
        assert alice["birthDate"] == "1980-03"

    def test_relationships_csv_content(self, export_dir):
        """Relationships CSV contains correct data."""
        export_to_csv(SAMPLE_FILE, export_dir)

        with open(Path(export_dir) / "relationships.csv", newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3

        alice_rel = next(r for r in rows if r["sourceRecordId"] == "rec-person-alice")
        assert alice_rel["targetRecordId"] == "rec-entity-alpha"
        assert alice_rel["recordId"] == "rec-rel-alice-alpha"

    def test_import_script_executable(self, export_dir):
        """Import shell script should be executable."""
        export_to_csv(SAMPLE_FILE, export_dir)
        script_path = Path(export_dir) / "import.sh"
        assert os.access(script_path, os.X_OK)

    def test_cypher_script_contains_constraints(self, export_dir):
        """Cypher script includes constraint creation."""
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (Path(export_dir) / "import.cypher").read_text()

        assert "CREATE CONSTRAINT" in script
        assert "constraint_entity_record_id" in script
        assert "constraint_person_record_id" in script
        assert "LOAD CSV" in script
        assert "HAS_INTEREST" in script
