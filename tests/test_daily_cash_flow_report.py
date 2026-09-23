"""Tests for DailyCashFlowReport v2 day-column chunking."""

import datetime
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.DailyCashFlowReport import (
    DailyCashFlowReport,
    _is_empty_or_zero_daily_value,
)
class TestIsEmptyOrZeroDailyValue:
    @pytest.mark.parametrize(
        "value",
        ["", None, "0", "0.0", "0.00", "0,00", 0, 0.0],
    )
    def test_zero_like_values(self, value):
        assert _is_empty_or_zero_daily_value(value) is True

    @pytest.mark.parametrize("value", ["1.00", "-1.00", "0.01"])
    def test_non_zero_values(self, value):
        assert _is_empty_or_zero_daily_value(value) is False


class TestDailyCashFlowReportSync:
    @pytest.fixture
    def report(self, mock_qb):
        mock_qb.report_period_days = None
        return DailyCashFlowReport(
            qb=mock_qb,
            start_date=datetime.datetime(2025, 1, 1),
        )

    @staticmethod
    def _cashflow_response(day_labels):
        return {
            "Columns": {
                "Column": [
                    {"ColTitle": "", "ColType": "Account"},
                    *[{"ColTitle": label, "ColType": "Money"} for label in day_labels],
                    {"ColTitle": "Total", "ColType": "Money"},
                ]
            },
            "Rows": {
                "Row": [
                    {
                        "ColData": [
                            {"value": "Net Income"},
                            *[{"value": "1.00"} for _ in day_labels],
                            {"value": "10.00"},
                        ]
                    }
                ]
            },
        }

    def test_sync_chunks_requests_and_merges_daily_columns(self, report):
        calls = []

        def fake_get(report_entity, params):
            calls.append(params)
            start = datetime.datetime.strptime(params["start_date"], "%Y-%m-%d")
            end = datetime.datetime.strptime(params["end_date"], "%Y-%m-%d")
            days = (end - start).days + 1
            offset = (start - datetime.datetime(2025, 1, 1)).days
            labels = [f"Day{offset + i}" for i in range(days)]
            return self._cashflow_response(labels)

        report._get = fake_get
        current = datetime.datetime(2025, 12, 31)

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.DailyCashFlowReport.datetime"
        ) as mock_dt:
            mock_dt.datetime.now.return_value = current + timedelta(days=1)
            mock_dt.timedelta = timedelta
            records = list(report.sync(catalog_entry=None))

        assert len(calls) == 2
        assert calls[0]["start_date"] == "2025-01-01"
        assert calls[0]["end_date"] == "2025-07-19"
        assert calls[1]["start_date"] == "2025-07-20"
        assert calls[1]["end_date"] == "2025-12-31"
        assert all(c["summarize_column_by"] == "Days" for c in calls)

        assert len(records) == 1
        day_keys = set()
        for entry in records[0]["DailyTotal"]:
            day_keys.update(entry.keys())
        assert len(day_keys) == 365
        assert "Other" not in day_keys

    def test_parse_and_yield_rows_skips_zero_daily_values(self, report):
        resp = {
            "Columns": {
                "Column": [
                    {"ColTitle": "", "ColType": "Account"},
                    {"ColTitle": "2026-01-01", "ColType": "Money"},
                    {"ColTitle": "2026-01-02", "ColType": "Money"},
                    {"ColTitle": "Total", "ColType": "Money"},
                ]
            },
            "Rows": {
                "Row": [
                    {
                        "ColData": [
                            {"value": "Net Income"},
                            {"value": "0.00"},
                            {"value": "5.00"},
                            {"value": "5.00"},
                        ]
                    }
                ]
            },
        }
        columns = report._get_column_metadata(resp)
        records = list(report._parse_and_yield_rows(resp, columns))

        assert len(records) == 1
        assert records[0]["Total"] == 5.0
        assert records[0]["DailyTotal"] == [{"2026-01-02": "5.00"}]

    def test_parse_and_yield_rows_skips_missing_daily_values(self, report):
        resp = {
            "Columns": {
                "Column": [
                    {"ColTitle": "", "ColType": "Account"},
                    {"ColTitle": "2026-01-01", "ColType": "Money"},
                    {"ColTitle": "2026-01-02", "ColType": "Money"},
                    {"ColTitle": "Total", "ColType": "Money"},
                ]
            },
            "Rows": {
                "Row": [
                    {
                        "ColData": [
                            {"value": "Net Income"},
                            {"value": None},
                            {"value": "5.00"},
                            {"value": "5.00"},
                        ]
                    }
                ]
            },
        }
        columns = report._get_column_metadata(resp)
        records = list(report._parse_and_yield_rows(resp, columns))

        assert len(records) == 1
        assert records[0]["DailyTotal"] == [{"2026-01-02": "5.00"}]
