"""Unit tests for shared monthly report parsing and stream wiring."""

import datetime
from unittest.mock import patch

import pytest

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.quickbooks.reportstreams.MonthlyBalanceSheetReport import (
    MonthlyBalanceSheetReport,
)
from tap_quickbooks.quickbooks.reportstreams.MonthlyCashFlowReport import (
    MonthlyCashFlowReport,
)


class ConcreteReport(BaseReportStream):
    """Minimal concrete subclass used to exercise BaseReportStream methods directly."""

    tap_stream_id = "TestReport"
    stream = "TestReport"
    key_properties = []
    replication_method = "FULL_TABLE"

    def sync(self, catalog_entry):
        return iter([])


@pytest.fixture
def base_report(mock_qb):
    start_date = datetime.datetime(2024, 1, 1)
    return ConcreteReport(qb=mock_qb, start_date=start_date, report_periods=None)


class FixedDate(datetime.date):
    @classmethod
    def today(cls):
        return cls(2030, 4, 29)


class TestGetMonthlyColumnMetadata:
    def test_maps_qbo_columns_to_monthly_record_keys(self, base_report):
        cols = base_report._get_monthly_column_metadata(
            {
                "Columns": {
                    "Column": [
                        {"ColTitle": "", "ColType": "Account"},
                        {"ColTitle": "Memo/Description", "ColType": "Memo"},
                        {"ColTitle": "Jan 2024", "ColType": "Money"},
                        {"ColTitle": "Total", "ColType": "Money"},
                    ]
                }
            }
        )
        assert cols == ["Account", "Memo", "Jan2024", "Total", "Categories"]


class TestRecursiveRowSearch:
    def test_flattens_nested_rows_with_category_path(self, base_report):
        row = {
            "Header": {"ColData": [{"value": "Assets"}]},
            "Rows": {
                "Row": [
                    {
                        "Header": {"ColData": [{"value": "Bank Accounts"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "Checking"},
                                        {"value": "100.00"},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            },
        }
        categories = []
        output = []
        base_report._recursive_row_search(row, output, categories)
        assert output == [["Checking", "100.00", ["Assets", "Bank Accounts"]]]
        assert categories == []

    @pytest.mark.parametrize("row", [{"Rows": {}}, {"Rows": None}])
    def test_empty_group_rows_are_no_op(self, base_report, row):
        output = []
        base_report._recursive_row_search(row, output, [])
        assert output == []


class TestMergeRowIntoDict:
    COLS = ["Account", "Jan2024", "Feb2024", "Total", "Categories"]

    def test_merges_same_account_and_category_into_exact_monthly_record(self, base_report):
        merged = {}
        base_report._merge_row_into_dict(
            ["Checking", "100.00", "", "100.00", ["Assets"]],
            self.COLS,
            merged,
            track_total=False,
        )
        base_report._merge_row_into_dict(
            ["Checking", "", "200.00", "200.00", ["Assets"]],
            self.COLS,
            merged,
            track_total=False,
        )
        assert merged == {
            ("Checking", ("Assets",)): {
                "Account": "Checking",
                "Categories": ["Assets"],
                "MonthlyTotal": [
                    {"Jan2024": "100.00"},
                    {"Total": "100.00"},
                    {"Feb2024": "200.00"},
                    {"Total": "200.00"},
                ],
            }
        }

    def test_track_total_sums_total_and_excludes_it_from_monthly_entries(self, base_report):
        merged = {}
        base_report._merge_row_into_dict(
            ["Checking", "100.00", "", "100.00", ["Assets"]],
            self.COLS,
            merged,
            track_total=True,
        )
        base_report._merge_row_into_dict(
            ["Checking", "", "200.00", "200.00", ["Assets"]],
            self.COLS,
            merged,
            track_total=True,
        )
        assert merged == {
            ("Checking", ("Assets",)): {
                "Account": "Checking",
                "Categories": ["Assets"],
                "MonthlyTotal": [{"Jan2024": "100.00"}, {"Feb2024": "200.00"}],
                "Total": 300.0,
            }
        }

    @pytest.mark.parametrize(
        "raw_row,track_total",
        [
            (["Checking", "", "", "", []], False),
            (["Checking", "100.00", "", "", []], True),
        ],
    )
    def test_skips_rows_without_required_amounts(self, base_report, raw_row, track_total):
        merged = {}
        base_report._merge_row_into_dict(raw_row, self.COLS, merged, track_total)
        assert merged == {}


class TestFetchChunkRows:
    START = datetime.date(2024, 1, 1)
    END = datetime.date(2028, 12, 31)

    def test_returns_none_when_rows_key_missing(self, base_report):
        resp = {"Columns": {"Column": []}, "Rows": {}}
        with patch.object(base_report, "_get", return_value=resp):
            assert base_report._fetch_chunk_rows("BalanceSheet", "BS", self.START, self.END) is None

    def test_fetches_params_columns_and_flattened_rows(self, base_report):
        resp = {
            "Columns": {
                "Column": [
                    {"ColTitle": "", "ColType": "Account"},
                    {"ColTitle": "Jan 2024", "ColType": "Money"},
                ]
            },
            "Rows": {
                "Row": [
                    {
                        "Header": {"ColData": [{"value": "Assets"}]},
                        "Rows": {
                            "Row": [
                                {
                                    "ColData": [
                                        {"value": "Checking"},
                                        {"value": "100.00"},
                                    ]
                                }
                            ]
                        },
                    }
                ]
            },
        }
        with patch.object(base_report, "_get", return_value=resp) as mock_get:
            result = base_report._fetch_chunk_rows("BalanceSheet", "BS", self.START, self.END)
        assert result == (
            ["Account", "Jan2024", "Categories"],
            [["Checking", "100.00", ["Assets"]]],
        )
        params = mock_get.call_args.kwargs["params"]
        assert params["start_date"] == "2024-01-01"
        assert params["end_date"] == "2028-12-31"
        assert params["accounting_method"] == "Accrual"
        assert params["summarize_column_by"] == "Month"


class TestSyncMonthlyChunked:
    def _without_timestamp(self, record):
        assert record.pop("SyncTimestampUtc")
        return record

    def test_default_five_year_chunks_merge_balance_sheet_records(self, base_report):
        chunks = [
            (["Account", "Jan2024", "Categories"], [["Checking", "100.00", ["Assets"]]]),
            (["Account", "Jan2029", "Categories"], [["Checking", "200.00", ["Assets"]]]),
        ]
        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_fetch_chunk_rows", side_effect=chunks) as mock_fetch:
            records = list(base_report._sync_monthly_chunked("BalanceSheet", "BS", track_total=False))

        assert [call.args[2:] for call in mock_fetch.call_args_list] == [
            (datetime.date(2024, 1, 1), datetime.date(2028, 12, 31)),
            (datetime.date(2029, 1, 1), datetime.date(2030, 4, 29)),
        ]
        assert [self._without_timestamp(record) for record in records] == [
            {
                "Account": "Checking",
                "Categories": ["Assets"],
                "MonthlyTotal": [{"Jan2024": "100.00"}, {"Jan2029": "200.00"}],
            }
        ]

    def test_custom_chunk_years_control_chunk_boundaries(self, mock_qb):
        report = ConcreteReport(
            qb=mock_qb,
            start_date=datetime.datetime(2024, 1, 1),
            report_periods=None,
            monthly_report_chunk_years=2,
        )
        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(report, "_fetch_chunk_rows", return_value=None) as mock_fetch:
            records = list(report._sync_monthly_chunked("BalanceSheet", "BS", track_total=False))

        assert records == []
        assert [call.args[2:] for call in mock_fetch.call_args_list] == [
            (datetime.date(2024, 1, 1), datetime.date(2025, 12, 31)),
            (datetime.date(2026, 1, 1), datetime.date(2027, 12, 31)),
            (datetime.date(2028, 1, 1), datetime.date(2029, 12, 31)),
            (datetime.date(2030, 1, 1), datetime.date(2030, 4, 29)),
        ]

    def test_chunk_years_must_be_positive(self, mock_qb):
        with pytest.raises(ValueError, match="monthly_report_chunk_years"):
            ConcreteReport(
                qb=mock_qb,
                start_date=datetime.datetime(2024, 1, 1),
                report_periods=None,
                monthly_report_chunk_years=-1,
            )

    def test_cash_flow_tracks_rounded_total_separately(self, base_report):
        chunks = [
            (
                ["Account", "Jan2024", "Total", "Categories"],
                [["Operations", "100.005", "100.005", []]],
            ),
            (
                ["Account", "Jan2029", "Total", "Categories"],
                [["Operations", "200.005", "200.005", []]],
            ),
        ]
        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_fetch_chunk_rows", side_effect=chunks):
            records = list(base_report._sync_monthly_chunked("CashFlow", "CF", track_total=True))

        assert [self._without_timestamp(record) for record in records] == [
            {
                "Account": "Operations",
                "Categories": [],
                "MonthlyTotal": [{"Jan2024": "100.005"}, {"Jan2029": "200.005"}],
                "Total": round(100.005 + 200.005, 2),
            }
        ]

    @pytest.mark.parametrize("chunk", [None, (["Account", "Jan2024", "Categories"], [["Checking", "", []]])])
    def test_empty_chunks_yield_no_records(self, base_report, chunk):
        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_fetch_chunk_rows", return_value=chunk):
            records = list(base_report._sync_monthly_chunked("BalanceSheet", "BS", track_total=False))

        assert records == []


class TestMonthlyReportStreams:
    def test_monthly_balance_sheet_uses_shared_chunked_sync(self, mock_qb):
        report = MonthlyBalanceSheetReport(
            qb=mock_qb,
            start_date=datetime.datetime(2024, 1, 1),
            report_periods=None,
        )
        with patch.object(report, "_sync_monthly_chunked", return_value=iter(["record"])) as mock_sync:
            assert list(report.sync(catalog_entry=None)) == ["record"]
        mock_sync.assert_called_once_with(
            report_entity="BalanceSheet",
            log_name="MonthlyBalanceSheet",
            track_total=False,
        )

    def test_monthly_cash_flow_uses_shared_chunked_sync_with_total_tracking(self, mock_qb):
        report = MonthlyCashFlowReport(
            qb=mock_qb,
            start_date=datetime.datetime(2024, 1, 1),
            report_periods=None,
        )
        with patch.object(report, "_sync_monthly_chunked", return_value=iter(["record"])) as mock_sync:
            assert list(report.sync(catalog_entry=None)) == ["record"]
        mock_sync.assert_called_once_with(
            report_entity="CashFlow",
            log_name="MonthlyCashFlow",
            track_total=True,
        )
