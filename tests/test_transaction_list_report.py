"""Tests for TransactionListReport response validation and retries (HGI-11578)."""

import datetime
from unittest.mock import MagicMock, patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.TransactionListReport import TransactionListReport
from tap_quickbooks.quickbooks.rest_reports import RetriableException
from tests.fixtures.streams.report import load_fixture


@pytest.fixture
def stream():
    start = datetime.datetime(2024, 1, 1)
    return TransactionListReport(qb=MagicMock(), start_date=start, report_periods=None)


@pytest.fixture
def catalog_entry():
    return {"stream": "TransactionListReport", "tap_stream_id": "TransactionListReport"}


class TestTransactionListReportValidation:
    def test_fault_raises_without_retry(self, stream):
        resp = load_fixture("transaction_list_fault.json")
        with pytest.raises(Exception, match="QuickBooks TransactionList error"):
            stream._validate_transaction_list_response(resp)

    def test_missing_columns_raises_retriable(self, stream):
        with pytest.raises(RetriableException, match="missing Columns"):
            stream._validate_transaction_list_response({})


class TestTransactionListReportSync:
    def test_yields_record_when_response_valid(self, stream, catalog_entry):
        good = load_fixture("transaction_list_response.json")
        with patch.object(stream, "_fetch_transaction_list", return_value=good):
            records = list(stream.sync(catalog_entry))
        assert len(records) == 1
        assert records[0]["Amount"] == 42.5

    def test_retries_then_succeeds(self, stream, catalog_entry):
        good = load_fixture("transaction_list_response.json")
        side_effects = [{}, good]

        def fake_execute(_entity, _params):
            body = side_effects.pop(0)
            return body

        with patch.object(stream, "_execute_request", side_effect=fake_execute):
            records = list(stream.sync(catalog_entry))
        assert len(records) == 1

    def test_fault_fails_sync_without_retry(self, stream, catalog_entry):
        fault = load_fixture("transaction_list_fault.json")
        with patch.object(stream, "_execute_request", return_value=fault) as mock_execute:
            with pytest.raises(Exception, match="QuickBooks TransactionList error"):
                list(stream.sync(catalog_entry))
        assert mock_execute.call_count == 1

    def test_empty_rows_returns_no_records(self, stream, catalog_entry):
        resp = load_fixture("transaction_list_response.json")
        resp = {**resp, "Rows": {}}
        with patch.object(stream, "_fetch_transaction_list", return_value=resp):
            assert list(stream.sync(catalog_entry)) == []
