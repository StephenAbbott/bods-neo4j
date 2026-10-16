"""Neo4j property types for the graph-native BODS schema.

One source of truth for which node / edge properties are not plain strings,
shared by both forward loaders so they build the same graph:

* ``bods_to_neo4j.driver_loader`` passes Python values straight to the
  driver, so floats / ints / bools are already typed; it uses
  :func:`to_neo4j_value` only to turn full ISO dates into ``datetime.date``.
* ``bods_to_neo4j.csv_exporter`` writes CSV, and ``LOAD CSV`` reads every
  cell as a string. Its generated ``import.cypher`` therefore casts each
  column with :func:`cypher_cast` instead of ``SET n += row``.

Without the casts, ``shareMinimum`` lands in Neo4j as ``"75.0"`` and
``isComponent`` as ``"false"`` — a string that is truthy in every client
and sorts lexically ("100.0" < "25.0").
"""

from __future__ import annotations

import datetime as _dt
import re

FLOAT_PROPS = frozenset({
    "shareExact",
    "shareMinimum",
    "shareMaximum",
})

# BODS share.exclusiveMinimum / exclusiveMaximum are numbers in v0.4, but
# some producers emit booleans. The mapper preserves either verbatim.
NUMBER_OR_BOOL_PROPS = frozenset({
    "shareExclusiveMin",
    "shareExclusiveMax",
})

INT_PROPS = frozenset({
    "interestIndex",
    "ordinal",
})

BOOL_PROPS = frozenset({
    "isComponent",
    "beneficialOwnershipOrControl",
    "synthetic",
    "isPrimary",
    "hasPublicListing",
    "addressEmpty",
    "annotationsKeyPresent",
})

# Only BODS fields whose schema format is a full ``date`` (YYYY-MM-DD).
# ``birthDate`` / ``deathDate`` are deliberately absent: BODS allows partial
# dates there ("1970", "1970-06"), and ``date("1970")`` would silently
# become 1970-01-01 and break the round trip.
DATE_PROPS = frozenset({
    "statementDate",
    "publicationDate",
    "startDate",
    "endDate",
    "foundingDate",
    "dissolutionDate",
    "formedByStatuteDate",
})

_FULL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Same check inside Cypher (Java regex, whole-string match).
_CYPHER_FULL_DATE_RE = r"\\d{4}-\\d{2}-\\d{2}"


def to_neo4j_value(key: str, value):
    """Convert one forward-mapped property value to its Neo4j-native type.

    Full ISO dates on :data:`DATE_PROPS` become ``datetime.date`` (stored as
    a Neo4j ``DATE``). Anything that is not a well-formed full date is left
    as it is, so a malformed source value is kept rather than dropped.
    """
    if key in DATE_PROPS and isinstance(value, str) and _FULL_DATE_RE.match(value):
        try:
            return _dt.date.fromisoformat(value)
        except ValueError:
            return value
    return value


def cypher_cast(key: str, var: str = "row") -> str:
    """Cypher expression reading CSV column ``key`` from ``var`` with its type.

    ``LOAD CSV`` yields ``null`` for empty unquoted cells, and ``toFloat`` /
    ``toInteger`` / ``toBoolean`` return ``null`` for null or unparseable
    input, so these casts are null-safe without extra guards. ``date()``
    raises on bad input, so it is guarded by a full-date regex and falls
    back to the raw string.
    """
    col = f"{var}.`{key}`" if not key.isidentifier() else f"{var}.{key}"
    if key in FLOAT_PROPS:
        return f"toFloat({col})"
    if key in INT_PROPS:
        return f"toInteger({col})"
    if key in BOOL_PROPS:
        return f"toBoolean({col})"
    if key in NUMBER_OR_BOOL_PROPS:
        return (
            f"CASE WHEN toLower({col}) IN ['true', 'false'] "
            f"THEN toBoolean({col}) ELSE toFloat({col}) END"
        )
    if key in DATE_PROPS:
        return (
            f"CASE WHEN {col} =~ '{_CYPHER_FULL_DATE_RE}' "
            f"THEN date({col}) ELSE {col} END"
        )
    return col


def to_plain(value):
    """Turn Neo4j / Python temporal values back into ISO strings, recursively.

    Used by the reverse extractor so BODS JSON output never sees a
    ``neo4j.time.Date``.
    """
    if isinstance(value, dict):
        return {k: to_plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_plain(v) for v in value]
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.isoformat()
    iso = getattr(value, "iso_format", None)
    if callable(iso):  # neo4j.time.Date / DateTime
        return iso()
    return value
