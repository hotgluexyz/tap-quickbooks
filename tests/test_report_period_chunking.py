"""Tests for Reports API period column chunking helpers."""

import datetime
from datetime import date, timedelta

from tap_quickbooks.quickbooks.reportstreams.report_period_chunking import (
    MAX_DAYS_PER_REQUEST,
    MAX_MONTHS_PER_REQUEST,
    iter_day_chunks,
    iter_month_chunks,
    merge_period_column_record,
)


class TestIterMonthChunks:
    def test_under_limit_is_single_chunk(self):
        start = date(2024, 1, 1)
        end = date(2024, 6, 30)
        assert list(iter_month_chunks(start, end)) == [(start, end)]

    def test_two_hundred_months_is_single_chunk(self):
        start = date(2005, 1, 1)
        end = date(2021, 8, 31)
        chunks = list(iter_month_chunks(start, end, max_months=MAX_MONTHS_PER_REQUEST))
        assert len(chunks) == 1
        assert chunks[0] == (start, end)

    def test_two_hundred_one_months_splits(self):
        start = date(2005, 1, 1)
        end = date(2021, 9, 30)
        chunks = list(iter_month_chunks(start, end, max_months=MAX_MONTHS_PER_REQUEST))
        assert len(chunks) == 2
        assert chunks[0][0] == start
        assert chunks[0][1] == date(2021, 8, 31)
        assert chunks[1] == (date(2021, 9, 1), end)


class TestIterDayChunks:
    def test_single_chunk_under_limit(self):
        start = datetime.datetime(2026, 1, 1)
        end = datetime.datetime(2026, 1, 31)
        assert list(iter_day_chunks(start, end, max_days=200)) == [(start, end)]

    def test_exactly_max_days_is_one_chunk(self):
        start = datetime.datetime(2026, 1, 1)
        end = start + timedelta(days=MAX_DAYS_PER_REQUEST - 1)
        chunks = list(iter_day_chunks(start, end, max_days=MAX_DAYS_PER_REQUEST))
        assert len(chunks) == 1
        assert chunks[0] == (start, end)

    def test_max_plus_one_splits_into_two_chunks(self):
        start = datetime.datetime(2026, 1, 1)
        end = start + timedelta(days=MAX_DAYS_PER_REQUEST)
        chunks = list(iter_day_chunks(start, end, max_days=MAX_DAYS_PER_REQUEST))
        assert len(chunks) == 2
        assert chunks[0][0] == start
        assert chunks[0][1] == start + timedelta(days=MAX_DAYS_PER_REQUEST - 1)
        assert chunks[1] == (chunks[0][1] + timedelta(days=1), end)

    def test_year_long_range_produces_expected_chunk_count(self):
        start = datetime.datetime(2025, 1, 1)
        end = datetime.datetime(2025, 12, 31)
        chunks = list(iter_day_chunks(start, end, max_days=200))
        assert len(chunks) == 2
        assert (chunks[0][1] - chunks[0][0]).days == 199
        assert chunks[1][1] == end


class TestMergePeriodColumnRecord:
    def test_merges_period_arrays_and_totals(self):
        merged = {}
        merge_period_column_record(
            merged,
            {
                "Account": "Sample",
                "Categories": ["A"],
                "Total": 1.0,
                "MonthlyTotal": [{"Jan2024": "1.00"}],
            },
            period_attr="MonthlyTotal",
            sum_total=True,
        )
        merge_period_column_record(
            merged,
            {
                "Account": "Sample",
                "Categories": ["A"],
                "Total": 2.0,
                "MonthlyTotal": [{"Feb2024": "2.00"}],
            },
            period_attr="MonthlyTotal",
            sum_total=True,
        )
        record = merged[("Sample", ("A",))]
        assert record["MonthlyTotal"] == [{"Jan2024": "1.00"}, {"Feb2024": "2.00"}]
        assert record["Total"] == 3.0

    def test_preserves_other_period_bucket(self):
        merged = {}
        merge_period_column_record(
            merged,
            {
                "Account": "Sample",
                "Categories": [],
                "MonthlyTotal": [{"Jan2024": "1.00"}, {"Other": "9.00"}],
            },
            period_attr="MonthlyTotal",
            sum_total=False,
        )
        assert merged[("Sample", ())]["MonthlyTotal"] == [
            {"Jan2024": "1.00"},
            {"Other": "9.00"},
        ]
