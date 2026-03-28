"""Tests for api.dependencies query functions."""

from __future__ import annotations

import io
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from api.dependencies import _query_parquet_bytes


def _make_parquet(rows: list[dict]) -> bytes:
    """Build an in-memory parquet from a list of dicts."""
    table = pa.table(
        {
            "date": [r["date"] for r in rows],
            "value": [r["value"] for r in rows],
            "series": [r["series"] for r in rows],
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


ANNUAL_ROWS = [
    {"date": date(2020, 1, 1), "value": 1.0, "series": "wb_test"},
    {"date": date(2021, 1, 1), "value": 2.0, "series": "wb_test"},
    {"date": date(2022, 1, 1), "value": 3.0, "series": "wb_test"},
    {"date": date(2023, 1, 1), "value": 4.0, "series": "wb_test"},
    {"date": date(2024, 1, 1), "value": 5.0, "series": "wb_test"},
]

DAILY_ROWS = [
    {"date": date(2024, 1, 5), "value": 10.0, "series": "daily_test"},
    {"date": date(2024, 1, 15), "value": 20.0, "series": "daily_test"},
    {"date": date(2024, 1, 25), "value": 30.0, "series": "daily_test"},
    {"date": date(2024, 2, 5), "value": 40.0, "series": "daily_test"},
    {"date": date(2024, 2, 15), "value": 50.0, "series": "daily_test"},
    {"date": date(2024, 3, 10), "value": 60.0, "series": "daily_test"},
]


def test_query_no_filter_returns_all() -> None:
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after=None)
    assert len(result) == 5


def test_query_filter_within_range() -> None:
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after="2023-01-01")
    assert len(result) == 2
    assert result[0]["value"] == 4.0
    assert result[1]["value"] == 5.0


def test_query_filter_fallback_when_empty() -> None:
    """When date filter excludes all rows, fall back to full dataset."""
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after="2025-03-27")
    # Should return all rows as fallback instead of empty
    assert len(result) == 5
    assert result[0]["value"] == 1.0


def test_query_empty_parquet_returns_empty() -> None:
    """Truly empty parquet returns empty list, no fallback."""
    data = _make_parquet([])
    result = _query_parquet_bytes(data, after="2025-01-01")
    assert result == []


# --- group_by aggregation tests ---


def test_query_group_by_month() -> None:
    """Daily rows grouped by month should produce one row per month with AVG."""
    data = _make_parquet(DAILY_ROWS)
    result = _query_parquet_bytes(data, after=None, group_by="month")
    assert len(result) == 3  # Jan, Feb, Mar
    # Jan: avg(10, 20, 30) = 20.0
    assert result[0]["value"] == 20.0
    # Feb: avg(40, 50) = 45.0
    assert result[1]["value"] == 45.0
    # Mar: avg(60) = 60.0
    assert result[2]["value"] == 60.0


def test_query_group_by_year() -> None:
    """Annual rows grouped by year should produce one row per year."""
    data = _make_parquet(ANNUAL_ROWS)
    result = _query_parquet_bytes(data, after=None, group_by="year")
    assert len(result) == 5
    assert result[0]["value"] == 1.0
    assert result[4]["value"] == 5.0


def test_query_group_by_week() -> None:
    """Daily rows grouped by week should aggregate into ISO weeks."""
    data = _make_parquet(DAILY_ROWS)
    result = _query_parquet_bytes(data, after=None, group_by="week")
    # 6 daily points across ~10 weeks → at least 5 distinct weeks
    assert len(result) >= 5
    assert all(r["series"] == "daily_test" for r in result)


def test_query_group_by_day_unchanged() -> None:
    """group_by='day' should return the same result as group_by=None."""
    data = _make_parquet(DAILY_ROWS)
    result_none = _query_parquet_bytes(data, after=None, group_by=None)
    result_day = _query_parquet_bytes(data, after=None, group_by="day")
    assert len(result_none) == len(result_day)
    for a, b in zip(result_none, result_day):
        assert a["value"] == b["value"]


def test_query_group_by_with_filter_fallback() -> None:
    """Aggregated query with a filter that excludes all rows should fall back."""
    data = _make_parquet(DAILY_ROWS)
    result = _query_parquet_bytes(data, after="2025-01-01", group_by="month")
    # Fallback returns all data aggregated
    assert len(result) == 3
