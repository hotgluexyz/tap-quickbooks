"""Unit tests for BalanceSheetReport account ID extraction."""

import datetime
from unittest.mock import patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.BalanceSheetReport import BalanceSheetReport


@pytest.fixture
def balance_sheet_report(mock_qb, load_report_fixture):
    """BalanceSheetReport with mocked QB client."""
    return BalanceSheetReport(
        qb=mock_qb,
        start_date=datetime.datetime(2024, 1, 1),
        report_periods=None,
    )


class TestBalanceSheetReport:
    def test_sync_emits_account_id_from_coldata(self, balance_sheet_report, load_report_fixture):
        response = load_report_fixture("balance_sheet_response.json")
        with patch.object(balance_sheet_report, "_get", return_value=response):
            records = list(balance_sheet_report.sync(catalog_entry={}))

        assert len(records) == 2
        checking = next(r for r in records if r["Account"] == "Checking")
        savings = next(r for r in records if r["Account"] == "Savings")

        assert checking["AccountId"] == "35"
        assert checking["Total"] == 100.0
        assert checking["Categories"] == ["ASSETS", "Bank Accounts"]

        assert savings["AccountId"] == "36"
        assert savings["Total"] == 250.5

    def test_sync_skips_rows_without_total(self, balance_sheet_report, load_report_fixture):
        response = load_report_fixture("balance_sheet_response.json")
        with patch.object(balance_sheet_report, "_get", return_value=response):
            records = list(balance_sheet_report.sync(catalog_entry={}))

        assert all(r["Account"] != "Summary Row" for r in records)

    def test_object_definition_includes_account_id(self):
        from tap_quickbooks.quickbooks import QB_OBJECT_DEFINITIONS

        field_names = [f["name"] for f in QB_OBJECT_DEFINITIONS["BalanceSheetReport"]]
        assert "AccountId" in field_names
