"""Unit tests for shared monthly report parsing and stream wiring."""

import datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.quickbooks.reportstreams.MonthlyBalanceSheetReport import (
    MonthlyBalanceSheetReport,
)
from tap_quickbooks.quickbooks.reportstreams.MonthlyCashFlowReport import (
    MonthlyCashFlowReport,
)


def _make_504_error():
    resp = MagicMock()
    resp.status_code = 504
    err = requests.exceptions.HTTPError(response=resp)
    return err


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


class TestProcessPeriod:
    START = datetime.date(2024, 1, 1)
    END = datetime.date(2024, 12, 31)

    RESP = {
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
                            {"ColData": [{"value": "Checking"}, {"value": "100.00"}]}
                        ]
                    },
                }
            ]
        },
    }

    def test_accumulates_rows_into_merged_with_correct_params(self, base_report):
        merged = {}
        with patch.object(base_report, "_get_504_fatal", return_value=self.RESP) as mock_get:
            base_report._process_period("BalanceSheet", "BS", self.START, self.END, merged, track_total=False)

        assert ("Checking", ("Assets",)) in merged
        assert merged[("Checking", ("Assets",))]["MonthlyTotal"] == [{"Jan2024": "100.00"}]
        params = mock_get.call_args.kwargs["params"]
        assert params["start_date"] == "2024-01-01"
        assert params["end_date"] == "2024-12-31"
        assert params["accounting_method"] == "Accrual"
        assert params["summarize_column_by"] == "Month"

    def test_skips_empty_response(self, base_report):
        merged = {}
        with patch.object(base_report, "_get_504_fatal", return_value={"Columns": {"Column": []}, "Rows": {}}):
            base_report._process_period("BalanceSheet", "BS", self.START, self.END, merged, track_total=False)
        assert merged == {}

    def test_splits_on_504_and_retries_halves(self, base_report):
        starts_seen = []

        def get_side_effect(**kwargs):
            start = kwargs["params"]["start_date"]
            starts_seen.append(start)
            if start == "2024-01-01" and kwargs["params"]["end_date"] == "2024-12-31":
                raise _make_504_error()
            return self.RESP

        merged = {}
        with patch.object(base_report, "_get_504_fatal", side_effect=get_side_effect):
            base_report._process_period("BalanceSheet", "BS", self.START, self.END, merged, track_total=False)

        assert starts_seen[0] == "2024-01-01"
        assert "2024-01-01" in starts_seen[1:]
        assert "2024-07-01" in starts_seen

    def test_single_month_goes_directly_to_point_in_time_without_columnar_attempt(self, base_report):
        """A 1-month period skips summarize_column_by=Month entirely and uses PIT."""
        pit_resp = {
            "Columns": {"Column": [
                {"ColTitle": "", "ColType": "Account"},
                {"ColTitle": "Total", "ColType": "Money"},
            ]},
            "Rows": {"Row": [{"ColData": [{"value": "Checking"}, {"value": "500.00"}]}]},
        }

        merged = {}
        with patch.object(base_report, "_get_504_fatal", return_value=pit_resp) as mock_get:
            base_report._process_period(
                "BalanceSheet", "BS",
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                merged, track_total=False,
            )

        assert merged[("Checking", ())]["MonthlyTotal"] == [{"Jan2024": "500.00"}]
        params = mock_get.call_args.kwargs["params"]
        assert "summarize_column_by" not in params

    def test_raises_immediately_on_non_504_http_error(self, base_report):
        resp = MagicMock()
        resp.status_code = 401
        err = requests.exceptions.HTTPError(response=resp)
        merged = {}
        with patch.object(base_report, "_get_504_fatal", side_effect=err):
            with pytest.raises(requests.exceptions.HTTPError):
                base_report._process_period("BalanceSheet", "BS", self.START, self.END, merged, track_total=False)


class TestProcessPeriodPointInTime:
    PIT_RESP = {
        "Columns": {"Column": [
            {"ColTitle": "", "ColType": "Account"},
            {"ColTitle": "Total", "ColType": "Money"},
        ]},
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "OPERATING ACTIVITIES"}]},
                    "Rows": {"Row": [
                        {"ColData": [{"value": "Net Income"}, {"value": "1500.00"}]},
                    ]},
                },
                {"ColData": [{"value": "Cash at beginning of period"}, {"value": "5000.00"}]},
            ]
        },
    }

    def test_cash_flow_skips_top_level_rows_with_no_category(self, base_report):
        merged = {}
        with patch.object(base_report, "_get_504_fatal", return_value=self.PIT_RESP):
            base_report._process_period_point_in_time(
                "CashFlow", "CF",
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                merged, track_total=True,
            )

        keys = list(merged.keys())
        assert ("Cash at beginning of period", ()) not in keys
        assert ("Net Income", ("OPERATING ACTIVITIES",)) in keys

    def test_balance_sheet_includes_top_level_rows_with_no_category(self, base_report):
        merged = {}
        with patch.object(base_report, "_get_504_fatal", return_value=self.PIT_RESP):
            base_report._process_period_point_in_time(
                "BalanceSheet", "BS",
                datetime.date(2024, 1, 1), datetime.date(2024, 1, 31),
                merged, track_total=False,
            )

        assert ("Cash at beginning of period", ()) in merged


class TestPointInTimeColName:
    def test_full_month_returns_monYYYY(self, base_report):
        assert base_report._point_in_time_col_name(
            datetime.date(2024, 2, 1), datetime.date(2024, 2, 29)
        ) == "Feb2024"

    def test_partial_month_matches_qbo_format(self, base_report):
        # QBO generates "Apr 1-29, 2026" → stripped → "Apr1-29,2026"
        assert base_report._point_in_time_col_name(
            datetime.date(2026, 4, 1), datetime.date(2026, 4, 29)
        ) == "Apr1-29,2026"

    def test_full_december_returns_dec(self, base_report):
        assert base_report._point_in_time_col_name(
            datetime.date(2023, 12, 1), datetime.date(2023, 12, 31)
        ) == "Dec2023"


class TestSyncMonthlyChunked:
    """FixedDate.today() returns 2030-04-29; base_report.start_date is 2024-01-01."""

    def _without_timestamp(self, record):
        assert record.pop("SyncTimestampUtc")
        return record

    def test_calls_process_period_with_full_range(self, base_report):
        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_process_period") as mock_process:
            list(base_report._sync_monthly_chunked("BalanceSheet", "BS", track_total=False))

        mock_process.assert_called_once_with(
            "BalanceSheet", "BS",
            datetime.date(2024, 1, 1), FixedDate.today(),
            {}, False,
        )

    def test_yields_records_after_process_period_fills_merged(self, base_report):
        def fill_merged(report_entity, log_name, start, end, merged, track_total):
            merged[("Checking", ("Assets",))] = {
                "Account": "Checking",
                "Categories": ["Assets"],
                "MonthlyTotal": [{"Jan2024": "100.00"}],
            }

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_process_period", side_effect=fill_merged):
            records = list(base_report._sync_monthly_chunked("BalanceSheet", "BS", track_total=False))

        assert len(records) == 1
        r = records[0]
        assert r["Account"] == "Checking"
        assert r["MonthlyTotal"] == [{"Jan2024": "100.00"}]
        assert r["SyncTimestampUtc"]

    def test_cash_flow_rounds_total(self, base_report):
        def fill_merged(report_entity, log_name, start, end, merged, track_total):
            merged[("Operations", ())] = {
                "Account": "Operations",
                "Categories": [],
                "MonthlyTotal": [{"Jan2024": "100.005"}],
                "Total": 100.005,
            }

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_process_period", side_effect=fill_merged):
            records = list(base_report._sync_monthly_chunked("CashFlow", "CF", track_total=True))

        assert records[0]["Total"] == round(100.005, 2)

    def test_empty_monthly_total_is_not_yielded(self, base_report):
        def fill_merged(report_entity, log_name, start, end, merged, track_total):
            merged[("Checking", ())] = {
                "Account": "Checking",
                "Categories": [],
                "MonthlyTotal": [],
            }

        with patch(
            "tap_quickbooks.quickbooks.reportstreams.BaseReport.datetime.date",
            FixedDate,
        ), patch.object(base_report, "_process_period", side_effect=fill_merged):
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
