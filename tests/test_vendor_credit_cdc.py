"""Tests for VendorCredit deleted-record sync via CDC."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest
import singer.utils as singer_utils
from requests.exceptions import HTTPError

from tap_quickbooks.quickbooks import RetriableApiError
from tap_quickbooks.quickbooks.exceptions import TapQuickbooksException
from tap_quickbooks.quickbooks.rest import Rest, CDC_MAX_LOOKBACK_DAYS


def _mock_response(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    return resp


def _qb_with_cdc_responses(responses):
    qb = MagicMock()
    qb.include_deleted = True
    qb.instance_url = "https://sandbox-quickbooks.api.intuit.com/v3/company/123"
    qb._get_standard_headers.return_value = {"Authorization": "Bearer token"}
    if len(responses) == 1:
        qb._make_request.return_value = _mock_response(responses[0])
    else:
        qb._make_request.side_effect = [_mock_response(r) for r in responses]
    return qb


def _recent_cdc_start():
    return singer_utils.strftime(singer_utils.now() - timedelta(hours=1))


class TestVendorCreditCdc:
    def test_yields_only_deleted_records(self):
        payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "99", "status": "Deleted", "domain": "QBO"},
                        {
                            "Id": "100",
                            "DocNumber": "VC-1",
                            "MetaData": {"LastUpdatedTime": "2026-10-16T12:00:00-07:00"},
                        },
                    ],
                    "totalCount": 2,
                    "maxResults": 1000,
                },
            }],
        }
        rest = Rest(_qb_with_cdc_responses([payload]))
        deleted = list(rest._sync_vendor_credit_cdc_deletes(_recent_cdc_start()))

        assert len(deleted) == 1
        assert deleted[0]["Id"] == "99"
        assert deleted[0]["status"] == "Deleted"

    def test_paginates_when_total_count_exceeds_max_results(self):
        page_one = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "1", "status": "Deleted", "domain": "QBO"},
                        {
                            "Id": "2",
                            "MetaData": {"LastUpdatedTime": "2026-10-15T10:00:00-07:00"},
                        },
                    ],
                    "totalCount": 3,
                    "maxResults": 2,
                },
            }],
        }
        page_two = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "3", "status": "Deleted", "domain": "QBO"},
                    ],
                    "totalCount": 1,
                    "maxResults": 1000,
                },
            }],
        }
        empty_page = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [],
                    "totalCount": 0,
                    "maxResults": 1000,
                },
            }],
        }
        qb = _qb_with_cdc_responses([page_one, page_two] + [empty_page] * 8)
        rest = Rest(qb)
        deleted = list(rest._sync_vendor_credit_cdc_deletes(_recent_cdc_start()))

        assert [rec["Id"] for rec in deleted] == ["1", "3"]
        assert qb._make_request.call_count >= 2

    def test_clamps_changed_since_to_cdc_window(self):
        empty_payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [],
                    "totalCount": 0,
                    "maxResults": 1000,
                },
            }],
        }
        qb = _qb_with_cdc_responses([empty_payload])
        rest = Rest(qb)
        old_start = "2017-01-01T00:00:00Z"
        list(rest._sync_vendor_credit_cdc_deletes(old_start))

        params = qb._make_request.call_args_list[0].kwargs["params"]
        expected_earliest = singer_utils.now() - timedelta(days=CDC_MAX_LOOKBACK_DAYS)
        actual = singer_utils.strptime_with_tz(params["changedSince"])
        assert actual >= expected_earliest - timedelta(seconds=5)

    def test_deduplicates_deleted_ids_across_partitions(self):
        payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "99", "status": "Deleted", "domain": "QBO"},
                    ],
                    "totalCount": 1,
                    "maxResults": 1000,
                },
            }],
        }
        qb = _qb_with_cdc_responses([payload])
        rest = Rest(qb)
        start = singer_utils.strftime(singer_utils.now() - timedelta(hours=9))
        deleted = list(rest._sync_vendor_credit_cdc_deletes(start))
        assert len(deleted) == 1
        assert deleted[0]["Id"] == "99"

    def test_skips_subdivide_when_truncated_boundary_reaches_window_end(self):
        window_end = singer_utils.now()
        window_start = window_end - timedelta(hours=1)
        boundary_time = singer_utils.strftime(window_end)
        truncated_payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "1", "status": "Deleted", "domain": "QBO"},
                        {
                            "Id": "2",
                            "MetaData": {"LastUpdatedTime": boundary_time},
                        },
                    ],
                    "totalCount": 3,
                    "maxResults": 2,
                },
            }],
        }
        qb = _qb_with_cdc_responses([truncated_payload])
        rest = Rest(qb)
        url = f"{qb.instance_url}/cdc"
        headers = qb._get_standard_headers.return_value

        deleted = list(
            rest._yield_partition_cdc_deletes(
                url,
                headers,
                window_start,
                window_end,
                set(),
            ))

        assert [rec["Id"] for rec in deleted] == ["1"]
        assert qb._make_request.call_count == 1

    def test_raises_when_truncated_at_minimum_window(self):
        truncated_payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "1", "status": "Deleted", "domain": "QBO"},
                    ],
                    "totalCount": 1001,
                    "maxResults": 1000,
                },
            }],
        }
        qb = _qb_with_cdc_responses([truncated_payload])
        rest = Rest(qb)
        window_start = singer_utils.now()
        window_end = window_start + timedelta(seconds=1)
        url = f"{qb.instance_url}/cdc"
        headers = qb._get_standard_headers.return_value

        with pytest.raises(TapQuickbooksException, match="incomplete"):
            list(
                rest._yield_partition_cdc_deletes(
                    url,
                    headers,
                    window_start,
                    window_end,
                    set(),
                ))

    def test_cdc_failure_propagates(self):
        qb = MagicMock()
        qb.instance_url = "https://sandbox-quickbooks.api.intuit.com/v3/company/123"
        qb._get_standard_headers.return_value = {}
        qb._make_request.side_effect = RetriableApiError("bad request")
        rest = Rest(qb)

        with pytest.raises(RetriableApiError):
            list(rest._sync_vendor_credit_cdc_deletes(_recent_cdc_start()))

    def test_cdc_http_error_propagates(self):
        qb = MagicMock()
        qb.instance_url = "https://sandbox-quickbooks.api.intuit.com/v3/company/123"
        qb._get_standard_headers.return_value = {}
        qb._make_request.side_effect = HTTPError("server error")
        rest = Rest(qb)

        with pytest.raises(HTTPError):
            list(rest._sync_vendor_credit_cdc_deletes(_recent_cdc_start()))

    def test_parse_metadata_handles_null_and_json_string(self):
        assert Rest._parse_metadata({}) == {}
        assert Rest._parse_metadata({"MetaData": None}) == {}
        assert Rest._parse_metadata({
            "MetaData": '{"LastUpdatedTime": "2026-10-15T10:00:00-07:00"}',
        }) == {"LastUpdatedTime": "2026-10-15T10:00:00-07:00"}

    def test_deleted_record_without_metadata_gets_empty_object(self):
        payload = {
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [
                        {"Id": "99", "status": "Deleted", "domain": "QBO"},
                    ],
                    "totalCount": 1,
                    "maxResults": 1000,
                },
            }],
        }
        rest = Rest(_qb_with_cdc_responses([payload]))
        deleted = list(rest._sync_vendor_credit_cdc_deletes(_recent_cdc_start()))
        assert deleted[0]["MetaData"] == {}
