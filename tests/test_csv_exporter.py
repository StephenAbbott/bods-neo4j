"""Tests for the graph-native CSV exporter."""

import csv
import os
import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.csv_exporter import export_to_csv

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


@pytest.fixture
def export_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def _rows(path: Path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


class TestCsvExport:
    def test_export_creates_node_csvs(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        for fname in ("entity.csv", "person.csv", "identifier.csv", "country.csv"):
            assert (export_dir / fname).exists(), f"missing {fname}"
        # No reified :Interest node CSV.
        assert not (export_dir / "interest.csv").exists()

    def test_export_creates_edge_csvs(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        # 3 relationship statements, all shareholding interests → OWNS edges
        assert (export_dir / "owns.csv").exists()
        # No :IN edge — typed edges go directly party→subject.
        assert not (export_dir / "in.csv").exists()
        assert (export_dir / "has_identifier.csv").exists()
        assert (export_dir / "has_address.csv").exists()
        assert (export_dir / "registered_in.csv").exists()

    def test_export_writes_script_files(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        assert (export_dir / "import.cypher").exists()
        assert (export_dir / "import.sh").exists()
        assert os.access(export_dir / "import.sh", os.X_OK)

    def test_export_counts(self, export_dir):
        counts = export_to_csv(SAMPLE_FILE, export_dir)
        assert counts["entity_statements"] == 3
        assert counts["person_statements"] == 2
        assert counts["relationship_statements"] == 3
        # Three OWNS edges (3 relationship statements, each carrying one
        # shareholding interest).
        assert counts["edges"].get("OWNS") == 3
        assert "IN" not in counts["edges"]

    def test_entity_csv_has_expected_columns(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        rows = _rows(export_dir / "entity.csv")
        assert len(rows) == 3
        # No legacy `_json` columns
        for row in rows:
            for col in row.keys():
                assert not col.endswith("_json") or col == "extrasJson", (
                    f"unexpected legacy column {col}"
                )

    def test_person_csv_carries_inline_nationalities(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        rows = _rows(export_dir / "person.csv")
        alice = next(r for r in rows if r["recordId"] == "rec-person-alice")
        # JSON-encoded list in the CSV cell.
        assert '"GB"' in alice["nationalityCodes"]

    def test_owns_csv_links_party_directly_to_subject(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        rows = _rows(export_dir / "owns.csv")
        alice_owns = [r for r in rows if r["start_key"] == "rec-person-alice"]
        assert len(alice_owns) == 1
        # end_key points directly at the subject entity
        assert alice_owns[0]["end_key"] == "rec-entity-alpha"
        # Edge carries the interest payload inline
        assert alice_owns[0]["bodsInterestType"] == "shareholding"

    def test_import_script_contains_new_constraints(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "constraint_identifier_uid" in script
        assert "constraint_country_code" in script
        # No legacy HAS_INTEREST or Interest constraint.
        assert "HAS_INTEREST" not in script
        assert "constraint_interest_id" not in script
