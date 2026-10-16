"""Tests for VendorCredit deleted-record sync via CDC."""

from datetime import timedelta
from unittest.mock import MagicMock

import singer.utils as singer_utils

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
    qb._make_request.side_effect = [_mock_response(r) for r in responses]
    return qb


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
        deleted = list(rest._sync_vendor_credit_cdc_deletes("2026-10-01T00:00:00Z"))

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
        qb = _qb_with_cdc_responses([page_one, page_two])
        rest = Rest(qb)
        deleted = list(rest._sync_vendor_credit_cdc_deletes("2026-10-01T00:00:00Z"))

        assert [rec["Id"] for rec in deleted] == ["1", "3"]
        assert qb._make_request.call_count == 2
        second_call_params = qb._make_request.call_args_list[1].kwargs["params"]
        assert second_call_params["changedSince"] == "2026-10-15T10:00:00-07:00"

    def test_clamps_changed_since_to_cdc_window(self):
        qb = _qb_with_cdc_responses([{
            "CDCResponse": [{
                "QueryResponse": {
                    "VendorCredit": [],
                    "totalCount": 0,
                    "maxResults": 1000,
                },
            }],
        }])
        rest = Rest(qb)
        old_start = "2017-01-01T00:00:00Z"
        list(rest._sync_vendor_credit_cdc_deletes(old_start))

        params = qb._make_request.call_args.kwargs["params"]
        expected_earliest = singer_utils.now() - timedelta(days=CDC_MAX_LOOKBACK_DAYS)
        actual = singer_utils.strptime_with_tz(params["changedSince"])
        assert actual >= expected_earliest - timedelta(seconds=5)

    def test_cdc_failure_does_not_raise(self):
        class RetriableApiError(Exception):
            pass

        qb = MagicMock()
        qb.instance_url = "https://sandbox-quickbooks.api.intuit.com/v3/company/123"
        qb._get_standard_headers.return_value = {}
        qb._make_request.side_effect = RetriableApiError("bad request")
        rest = Rest(qb)

        assert list(rest._sync_vendor_credit_cdc_deletes("2026-10-01T00:00:00Z")) == []
