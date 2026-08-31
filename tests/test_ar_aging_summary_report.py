"""Tests for ARAgingSummaryReport column normalization (HGI-10077 item 9)."""

from unittest.mock import MagicMock

from tap_quickbooks.quickbooks.reportstreams.ARAgingSummaryReport import ARAgingSummaryReport


def _column_response(col_titles):
    return {
        "Columns": {
            "Column": [
                {"ColTitle": title, "ColType": "Customer" if title == "" else "Money"}
                for title in col_titles
            ]
        }
    }


class TestARAgingSummaryColumnMetadata:
    def test_v1_col_titles_unchanged(self):
        stream = ARAgingSummaryReport(qb=MagicMock(), start_date=MagicMock(), report_periods=None)
        resp = _column_response(
            ["", "Current", "1 - 30", "31 - 60", "61 - 90", "91 and over", "Total"]
        )
        assert stream._get_column_metadata(resp) == [
            "Customer",
            "Current",
            "1-30",
            "31-60",
            "61-90",
            "91andover",
            "Total",
        ]

    def test_v2_all_caps_bucket_normalized(self):
        stream = ARAgingSummaryReport(qb=MagicMock(), start_date=MagicMock(), report_periods=None)
        resp = _column_response(
            ["", "Current", "1 - 30", "31 - 60", "61 - 90", "91 AND OVER", "Total"]
        )
        assert stream._get_column_metadata(resp) == [
            "Customer",
            "Current",
            "1-30",
            "31-60",
            "61-90",
            "91andover",
            "Total",
        ]
