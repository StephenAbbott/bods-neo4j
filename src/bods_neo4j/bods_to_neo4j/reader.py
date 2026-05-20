"""BODS JSON/JSONL file reader with streaming support."""

import json
import logging
from pathlib import Path
from typing import Generator, Union

logger = logging.getLogger(__name__)


def read_bods_file(file_path: Union[str, Path]) -> Generator[dict, None, None]:
    """Read BODS statements from a JSON or JSONL file.

    Automatically detects format based on file extension and content:
    - .jsonl / .ndjson: One JSON object per line (streaming, memory-efficient)
    - .json: Either a JSON array of statements or a single statement

    Args:
        file_path: Path to BODS data file

    Yields:
        Individual BODS statement dictionaries
    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"BODS file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix in (".jsonl", ".ndjson"):
        yield from _read_jsonl(file_path)
    elif suffix == ".json":
        # Real-world BODS bulk dumps (GLEIF, UK PSC) often use `.json`
        # extension even when the content is JSONL (one statement per
        # line). Sniff the first non-whitespace char to pick the right
        # parser — `[` means JSON array, `{` means JSONL.
        if _looks_like_json_array(file_path):
            yield from _read_json(file_path)
        else:
            yield from _read_jsonl(file_path)
    else:
        # Try JSONL first (more memory-efficient), fall back to JSON
        try:
            yield from _read_jsonl(file_path)
        except json.JSONDecodeError:
            yield from _read_json(file_path)


def _looks_like_json_array(file_path: Path) -> bool:
    """Peek at the first non-whitespace character: `[` = JSON array."""
    with open(file_path, "r", encoding="utf-8") as f:
        # Read at most 1 KB to find the first non-whitespace char.
        chunk = f.read(1024)
    for ch in chunk:
        if ch.isspace():
            continue
        return ch == "["
    return False


def _read_jsonl(file_path: Path) -> Generator[dict, None, None]:
    """Read BODS statements from a JSONL file (one statement per line)."""
    count = 0
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                statement = json.loads(line)
                if isinstance(statement, dict):
                    yield statement
                    count += 1
                    if count % 100_000 == 0:
                        logger.info("Read %d statements from %s", count, file_path.name)
            except json.JSONDecodeError as e:
                logger.warning("Skipping invalid JSON at line %d: %s", line_num, e)

    logger.info("Finished reading %d statements from %s", count, file_path.name)


def _read_json(file_path: Path) -> Generator[dict, None, None]:
    """Read BODS statements from a JSON file (array or single object)."""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        logger.info("Reading %d statements from JSON array in %s", len(data), file_path.name)
        for statement in data:
            if isinstance(statement, dict):
                yield statement
    elif isinstance(data, dict):
        logger.info("Reading single statement from %s", file_path.name)
        yield data
    else:
        raise ValueError(f"Unexpected JSON structure in {file_path}: {type(data)}")


def count_statements(file_path: Union[str, Path]) -> dict:
    """Count statements by record type in a BODS file.

    Returns:
        Dictionary with counts: {"entity": N, "person": N, "relationship": N, "total": N}
    """
    counts = {"entity": 0, "person": 0, "relationship": 0, "other": 0, "total": 0}

    for statement in read_bods_file(file_path):
        record_type = statement.get("recordType", "other")
        if record_type in counts:
            counts[record_type] += 1
        else:
            counts["other"] += 1
        counts["total"] += 1

    return counts
