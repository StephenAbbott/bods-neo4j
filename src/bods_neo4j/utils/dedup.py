"""Pre-scan dedup filter shared between the driver loader and CSV exporter.

A single file walk builds a ``PreScan`` that identifies superseded BODS
statements; the exporter / loader then skips losers during streaming so
they never reach Neo4j (or never make it into the CSV files). This avoids
the need for any post-hoc dedup work on the graph side.

What "loser" means:

* **Older relationship version.** For relationship statements, the latest
  ``statementId`` per ``recordId`` wins. "Latest" = highest
  ``statementDate``, tiebreak lexicographic ``statementId`` so the choice
  is deterministic across runs.
* **Explicitly retired.** Any statement whose own ``statementId`` is named
  in some other statement's ``replacesStatements`` array (top-level or
  inside ``recordDetails``) is filtered out — covers entity/person
  collapse (new statement under a different ``recordId`` explicitly
  retires the old one) and any explicit relationship retirement.

Caveat — incremental loads: the pre-scan only sees the current file.
Re-running against a graph / CSV directory that already contains prior
state leaves the older state untouched. Use ``--clear`` on the loader for
a clean reload, or regenerate the CSV directory from scratch.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Union

from ..bods_to_neo4j.reader import read_bods_file

logger = logging.getLogger(__name__)


class PreScan:
    """Result of the pre-scan file walk used to filter superseded statements.

    Attributes:
        latest_statement_per_record: relationship ``recordId`` →
            winning ``statementId``.
        superseded_statement_ids: every ``statementId`` named in some
            statement's ``replacesStatements`` array.
    """
    __slots__ = ("latest_statement_per_record", "superseded_statement_ids")

    def __init__(self, latest_statement_per_record, superseded_statement_ids):
        self.latest_statement_per_record = latest_statement_per_record
        self.superseded_statement_ids = superseded_statement_ids


def scan_for_dedup(bods_file: Union[str, Path]) -> PreScan:
    """Walk the BODS file once and build the dedup-filter data.

    O(N) in Python with bounded memory: one dict keyed on relationship
    ``recordId`` and one set of superseded ``statementId``s.
    """
    latest: dict[str, tuple] = {}  # rid -> (statementDate, statementId)
    superseded: set[str] = set()
    n = 0
    for s in read_bods_file(bods_file):
        for sid in (s.get("replacesStatements") or []):
            if sid:
                superseded.add(sid)
        details = s.get("recordDetails") or {}
        for sid in (details.get("replacesStatements") or []):
            if sid:
                superseded.add(sid)

        if s.get("recordType") == "relationship":
            rid = s.get("recordId")
            sid = s.get("statementId")
            if rid and sid:
                sdate = s.get("statementDate") or ""
                existing = latest.get(rid)
                if existing is None or (sdate, sid) > existing:
                    latest[rid] = (sdate, sid)
        n += 1
        if n % 1_000_000 == 0:
            logger.info(
                "  pre-scan: %d statements, %d unique relationship recordIds, "
                "%d superseded statementIds so far",
                n, len(latest), len(superseded),
            )
    return PreScan(
        latest_statement_per_record={rid: sid for rid, (_d, sid) in latest.items()},
        superseded_statement_ids=superseded,
    )


def is_loser(statement: dict, pre_scan: PreScan) -> bool:
    """Should this statement be skipped during export / load?

    Returns True iff the statement is superseded — either explicitly via
    another statement's ``replacesStatements`` or implicitly by being an
    older version of the same relationship ``recordId``.
    """
    sid = statement.get("statementId")
    if sid and sid in pre_scan.superseded_statement_ids:
        return True
    if statement.get("recordType") == "relationship":
        rid = statement.get("recordId")
        if rid and sid and pre_scan.latest_statement_per_record.get(rid) != sid:
            return True
    return False
