"""Tests for the graph-native CSV exporter."""

import csv
import os
import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.csv_exporter import _flatten_for_csv, export_to_csv

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


class TestBackslashEscaping:
    """Backslashes in CSV cells must be doubled so Neo4j's default legacy
    quote escaping does not misinterpret backslash-quote as an escape."""

    def test_backslash_in_string_value_is_doubled(self):
        result = _flatten_for_csv({"name": 'Puppet \\"Investment\\"'})
        assert result["name"] == 'Puppet \\\\"Investment\\\\"'

    def test_backslashes_in_json_list_are_doubled(self):
        result = _flatten_for_csv({"alt": ['"\\"ТОО"']})
        assert "\\\\" in result["alt"]

    def test_plain_string_without_backslash_unchanged(self):
        result = _flatten_for_csv({"name": "Alpha Corp"})
        assert result["name"] == "Alpha Corp"

    def test_bool_and_none_unaffected(self):
        result = _flatten_for_csv({"flag": True, "empty": None})
        assert result["flag"] == "true"
        assert result["empty"] == ""

    def test_import_sh_documents_legacy_escaping(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.sh").read_text()
        assert "legacy_quote_escaping" in script


class TestTypedImport:
    """LOAD CSV reads every cell as a string, so import.cypher must cast typed
    columns instead of ``SET r += row`` (which stored shares as "75.0")."""

    def test_no_untyped_bulk_set(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "+= row" not in script

    def test_share_columns_cast_to_float(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "r.shareExact = toFloat(row.shareExact)" in script

    def test_booleans_and_ints_cast(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "n.isComponent = toBoolean(row.isComponent)" in script
        assert "r.isComponent = toBoolean(row.isComponent)" in script
        assert "r.interestIndex = toInteger(row.interestIndex)" in script

    def test_full_dates_cast_with_regex_guard(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert (
            "r.statementDate = CASE WHEN row.statementDate =~ "
            "'\\\\d{4}-\\\\d{2}-\\\\d{2}' THEN date(row.statementDate) "
            "ELSE row.statementDate END"
        ) in script

    def test_endpoint_keys_not_copied_onto_edges(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "r.start_key" not in script
        assert "r.end_key" not in script

    def test_merge_key_not_reset(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        script = (export_dir / "import.cypher").read_text()
        assert "n.recordId = row.recordId" not in script


class TestIdentifiersProperty:
    """Issue #3: opt-in denormalised ``identifiers`` on Entity/Person nodes."""

    def test_off_by_default(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir)
        for fname in ("entity.csv", "person.csv"):
            assert "identifiers" not in _rows(export_dir / fname)[0]
        assert "row.identifiers" not in (export_dir / "import.cypher").read_text()

    def test_written_as_separated_cell_and_split_on_load(self, export_dir):
        export_to_csv(SAMPLE_FILE, export_dir, identifiers_property=True)
        rows = [r for r in _rows(export_dir / "entity.csv") if r.get("identifiers")]
        assert rows, "sample has entity identifiers"
        for r in rows:
            for token in r["identifiers"].split("\x1f"):
                scheme, _, ident = token.partition(":")
                assert scheme and ident and scheme == scheme.lower()
        script = (export_dir / "import.cypher").read_text()
        assert "split(row.identifiers, '\\u001F')" in script
