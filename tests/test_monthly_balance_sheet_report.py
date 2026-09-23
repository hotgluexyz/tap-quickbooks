"""Unit tests for MonthlyBalanceSheetReport account ID extraction."""

import datetime
from datetime import date
from unittest.mock import patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.MonthlyBalanceSheetReport import (
    MonthlyBalanceSheetReport,
)


@pytest.fixture
def monthly_balance_sheet_report(mock_qb):
    """MonthlyBalanceSheetReport with mocked QB client."""
    return MonthlyBalanceSheetReport(
        qb=mock_qb,
        start_date=datetime.datetime(2024, 1, 1),
    )


class TestMonthlyBalanceSheetReport:
    def test_sync_emits_account_id_and_monthly_totals(self, monthly_balance_sheet_report, load_report_fixture):
        response = load_report_fixture("monthly_balance_sheet_response.json")
        with patch.object(monthly_balance_sheet_report, "_get", return_value=response):
            records = list(monthly_balance_sheet_report.sync(catalog_entry={}))

        assert len(records) == 1
        record = records[0]
        assert record["Account"] == "Checking"
        assert record["AccountId"] == "35"
        assert record["Categories"] == ["ASSETS", "Bank Accounts"]
        assert record["MonthlyTotal"] == [{"Jan2024": "100.00"}, {"Feb2024": "110.00"}]

    def test_sync_chunks_when_range_exceeds_month_limit(
        self, monthly_balance_sheet_report, load_report_fixture
    ):
        response = load_report_fixture("monthly_balance_sheet_response.json")
        calls = []

        def fake_get(report_entity, params):
            calls.append(params)
            return response

        monthly_balance_sheet_report.start_date = datetime.datetime(2005, 1, 1)
        with patch.object(monthly_balance_sheet_report, "_get", side_effect=fake_get):
            with patch(
                "tap_quickbooks.quickbooks.reportstreams.MonthlyBalanceSheetReport.datetime.date"
            ) as mock_date:
                mock_date.today.return_value = date(2021, 9, 30)
                list(monthly_balance_sheet_report.sync(catalog_entry={}))

        assert len(calls) == 2

    def test_object_definitions_include_account_id(self):
        from tap_quickbooks.quickbooks import QB_OBJECT_DEFINITIONS

        monthly_fields = [f["name"] for f in QB_OBJECT_DEFINITIONS["MonthlyBalanceSheetReport"]]
        pnl_fields = [f["name"] for f in QB_OBJECT_DEFINITIONS["ProfitAndLossReport"]]
        assert "AccountId" in monthly_fields
        assert "AccountId" in pnl_fields
