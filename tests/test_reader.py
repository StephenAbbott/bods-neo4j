"""Tests for BODS file reader."""

import json
import os
import tempfile
from pathlib import Path

import pytest

from bods_neo4j.bods_to_neo4j.reader import read_bods_file, count_statements

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FILE = FIXTURES_DIR / "sample_bods.json"


class TestReadBodsFile:
    """Tests for read_bods_file function."""

    def test_read_json_file(self):
        """Read statements from a JSON array file."""
        statements = list(read_bods_file(SAMPLE_FILE))
        assert len(statements) == 8
        # Check we get all record types
        types = [s["recordType"] for s in statements]
        assert types.count("entity") == 3
        assert types.count("person") == 2
        assert types.count("relationship") == 3

    def test_read_jsonl_file(self):
        """Read statements from a JSONL file."""
        # Create a temporary JSONL file from the fixture
        with open(SAMPLE_FILE) as f:
            data = json.load(f)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            for statement in data:
                tmp.write(json.dumps(statement) + "\n")
            tmp_path = tmp.name

        try:
            statements = list(read_bods_file(tmp_path))
            assert len(statements) == 8
        finally:
            os.unlink(tmp_path)

    def test_read_jsonl_with_empty_lines(self):
        """JSONL reader should skip empty lines."""
        with open(SAMPLE_FILE) as f:
            data = json.load(f)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            for i, statement in enumerate(data):
                tmp.write(json.dumps(statement) + "\n")
                if i % 2 == 0:
                    tmp.write("\n")  # Add empty line
            tmp_path = tmp.name

        try:
            statements = list(read_bods_file(tmp_path))
            assert len(statements) == 8
        finally:
            os.unlink(tmp_path)

    def test_file_not_found(self):
        """Should raise FileNotFoundError for missing files."""
        with pytest.raises(FileNotFoundError):
            list(read_bods_file("/nonexistent/path/bods.json"))

    def test_statement_structure(self):
        """Each statement should have required BODS fields."""
        for statement in read_bods_file(SAMPLE_FILE):
            assert "statementId" in statement
            assert "recordId" in statement
            assert "recordType" in statement
            assert statement["recordType"] in ("entity", "person", "relationship")
            assert "recordDetails" in statement


class TestCountStatements:
    """Tests for count_statements function."""

    def test_count_by_type(self):
        """Count statements grouped by record type."""
        counts = count_statements(SAMPLE_FILE)
        assert counts["entity"] == 3
        assert counts["person"] == 2
        assert counts["relationship"] == 3
        assert counts["total"] == 8
        assert counts["other"] == 0
