"""Tests for MonthlyCashFlowReport month-column chunking."""

import datetime
from datetime import date
from unittest.mock import patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.MonthlyCashFlowReport import (
    MonthlyCashFlowReport,
)
class TestMonthlyCashFlowReportSync:
    @pytest.fixture
    def report(self, mock_qb):
        return MonthlyCashFlowReport(
            qb=mock_qb,
            start_date=datetime.datetime(2005, 1, 1),
        )

    @staticmethod
    def _cashflow_response(month_labels):
        return {
            "Columns": {
                "Column": [
                    {"ColTitle": "", "ColType": "Account"},
                    *[
                        {"ColTitle": label, "ColType": "Money"}
                        for label in month_labels
                    ],
                    {"ColTitle": "Total", "ColType": "Money"},
                ]
            },
            "Rows": {
                "Row": [
                    {
                        "ColData": [
                            {"value": "Net Income"},
                            *[{"value": "1.00"} for _ in month_labels],
                            {"value": "10.00"},
                        ]
                    }
                ]
            },
        }

    def test_sync_chunks_long_range_and_omits_other(self, report):
        calls = []

        def fake_get(report_entity, params):
            calls.append(params)
            chunk_start = datetime.datetime.strptime(params["start_date"], "%Y-%m-%d").date()
            chunk_end = datetime.datetime.strptime(params["end_date"], "%Y-%m-%d").date()
            months = (
                (chunk_end.year - chunk_start.year) * 12
                + (chunk_end.month - chunk_start.month)
                + 1
            )
            offset = (chunk_start.year - 2005) * 12 + (chunk_start.month - 1)
            labels = [f"M{offset + i}" for i in range(months)]
            return self._cashflow_response(labels)

        report._get = fake_get
        end = date(2021, 9, 30)

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.MonthlyCashFlowReport.datetime.date"
        ) as mock_date:
            mock_date.today.return_value = end
            records = list(report.sync(catalog_entry=None))

        assert len(calls) == 2
        assert calls[0]["summarize_column_by"] == "Month"
        assert len(records) == 1
        month_keys = set()
        for entry in records[0]["MonthlyTotal"]:
            month_keys.update(entry.keys())
        assert "Other" not in month_keys
        assert len(month_keys) == (
            (end.year - 2005) * 12 + (end.month - 1) + 1
        )

    def test_sync_single_chunk_under_month_limit(self, report):
        report._get = lambda report_entity, params: self._cashflow_response(
            ["Jan2024", "Feb2024"]
        )
        report.start_date = datetime.datetime(2024, 1, 1)

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.MonthlyCashFlowReport.datetime.date"
        ) as mock_date:
            mock_date.today.return_value = date(2024, 2, 29)
            records = list(report.sync(catalog_entry=None))

        assert len(records) == 1
        assert records[0]["MonthlyTotal"] == [
            {"Jan2024": "1.00"},
            {"Feb2024": "1.00"},
        ]
