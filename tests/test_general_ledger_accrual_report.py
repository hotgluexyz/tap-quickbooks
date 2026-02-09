"""Unit tests for GeneralLedgerAccrualReport full_sync path."""

import datetime
from unittest.mock import patch

import pytest

from tests.fixtures.streams.report import minimal_gl_report_response
from tap_quickbooks.quickbooks.reportstreams.GeneralLedgerAccrualReport import (
    GeneralLedgerAccrualReport,
)


class TestGeneralLedgerAccrualReportFullSync:
    """Tests for GeneralLedgerAccrualReport when full_sync branch is taken (state_passed=False)."""

    def test_accounting_method_is_accrual(self):
        """Stream uses Accrual accounting method."""
        assert GeneralLedgerAccrualReport.accounting_method == "Accrual"

    def test_full_sync_includes_accrual_in_params(self, report, mock_qb, start_date, catalog_entry):
        """concurrent_get is called with params containing accounting_method Accrual."""
        start_str = start_date.strftime("%Y-%m-%d")
        mock_response = minimal_gl_report_response(start_str)

        with patch.object(
            report,
            "concurrent_get",
            return_value=mock_response,
        ) as mock_concurrent_get:
            list(report.sync(catalog_entry))

        assert mock_concurrent_get.call_count >= 1
        call_kwargs = mock_concurrent_get.call_args
        assert call_kwargs.kwargs["report_entity"] == "GeneralLedger"
        params = call_kwargs.kwargs["params"]
        assert params["accounting_method"] == "Accrual"
        assert "start_date" in params
        assert "end_date" in params
        assert params["sort_by"] == "tx_date"

    def test_full_sync_calls_concurrent_get_with_general_ledger(
        self, report, mock_qb, start_date, catalog_entry
    ):
        """Full sync uses report_entity GeneralLedger."""
        start_str = start_date.strftime("%Y-%m-%d")
        mock_response = minimal_gl_report_response(start_str)

        with patch.object(
            report,
            "concurrent_get",
            return_value=mock_response,
        ) as mock_concurrent_get:
            list(report.sync(catalog_entry))

        for call in mock_concurrent_get.call_args_list:
            assert call.kwargs["report_entity"] == "GeneralLedger"

    def test_full_sync_yields_records_when_rows_present(
        self, report, start_date, catalog_entry
    ):
        """Full sync yields cleansed records when the report has rows."""
        start_str = start_date.strftime("%Y-%m-%d")
        mock_response = minimal_gl_report_response(start_str)

        with patch.object(report, "concurrent_get", return_value=mock_response):
            records = list(report.sync(catalog_entry))

        assert len(records) >= 1
        row = records[0]
        assert "Date" in row or "tx_date" in row or "SyncTimestampUtc" in row
        assert "SyncTimestampUtc" in row

    def test_full_sync_yields_nothing_when_no_rows(
        self, report, catalog_entry, gl_report_empty_rows
    ):
        """Full sync yields no records for a period when Rows.Row is None."""
        with patch.object(report, "concurrent_get", return_value=gl_report_empty_rows):
            records = list(report.sync(catalog_entry))

        assert len(records) == 0

    def test_full_sync_skips_rows_without_amount(
        self, report, catalog_entry, gl_report_no_amount
    ):
        """Rows without Amount are skipped by clean_row."""
        with patch.object(report, "concurrent_get", return_value=gl_report_no_amount):
            records = list(report.sync(catalog_entry))

        assert len(records) == 0

    def test_full_sync_used_when_state_passed_false(self, mock_qb, start_date, catalog_entry):
        """Full sync branch runs when state_passed is False."""
        report = GeneralLedgerAccrualReport(
            qb=mock_qb,
            start_date=start_date,
            report_periods=None,
            state_passed=False,
        )
        start_str = start_date.strftime("%Y-%m-%d")
        mock_response = minimal_gl_report_response(start_str)

        with patch.object(report, "concurrent_get", return_value=mock_response) as mock_get:
            list(report.sync(catalog_entry))

        mock_get.assert_called()

    def test_full_sync_used_when_gl_full_sync_true(self, mock_qb, start_date, catalog_entry):
        """Full sync branch runs when qb.gl_full_sync is True even with state."""
        mock_qb.gl_full_sync = True
        report = GeneralLedgerAccrualReport(
            qb=mock_qb,
            start_date=start_date,
            report_periods=None,
            state_passed=True,  # state passed but gl_full_sync forces full sync
        )
        start_str = start_date.strftime("%Y-%m-%d")
        mock_response = minimal_gl_report_response(start_str)

        with patch.object(report, "concurrent_get", return_value=mock_response) as mock_get:
            list(report.sync(catalog_entry))

        mock_get.assert_called()
        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs["params"]["accounting_method"] == "Accrual"
