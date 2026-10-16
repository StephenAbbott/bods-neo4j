"""Tests for the shared Neo4j property-type map."""

import datetime as dt

from bods_neo4j.utils.property_types import cypher_cast, to_neo4j_value, to_plain


def test_full_dates_become_date_objects():
    assert to_neo4j_value("statementDate", "2024-03-01") == dt.date(2024, 3, 1)
    assert to_neo4j_value("endDate", "2020-12-31") == dt.date(2020, 12, 31)


def test_partial_or_malformed_dates_kept_verbatim():
    # birthDate allows partial dates in BODS and is never cast.
    assert to_neo4j_value("birthDate", "1970-06-01") == "1970-06-01"
    assert to_neo4j_value("startDate", "2015") == "2015"
    assert to_neo4j_value("startDate", "2015-13-45") == "2015-13-45"


def test_non_date_props_untouched():
    assert to_neo4j_value("shareMinimum", 75.0) == 75.0
    assert to_neo4j_value("name", "2024-03-01") == "2024-03-01"


def test_to_plain_round_trips_dates_recursively():
    value = {"node": {"statementDate": dt.date(2024, 3, 1)},
             "edges": [{"startDate": dt.date(2020, 1, 2), "shareMinimum": 25.0}]}
    assert to_plain(value) == {
        "node": {"statementDate": "2024-03-01"},
        "edges": [{"startDate": "2020-01-02", "shareMinimum": 25.0}],
    }


def test_cypher_casts():
    assert cypher_cast("shareMinimum") == "toFloat(row.shareMinimum)"
    assert cypher_cast("isComponent") == "toBoolean(row.isComponent)"
    assert cypher_cast("ordinal") == "toInteger(row.ordinal)"
    assert cypher_cast("name") == "row.name"
    assert "toBoolean" in cypher_cast("shareExclusiveMin")
    assert "toFloat" in cypher_cast("shareExclusiveMin")
