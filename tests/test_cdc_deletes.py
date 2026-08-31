"""Unit tests for CDC deleted-record retrieval and the 1000-object cap."""

from copy import deepcopy
from unittest.mock import MagicMock

import pytest

from tap_quickbooks.quickbooks.rest import CDC_MAX_RESULTS, Rest

pytestmark = pytest.mark.unit


def _cdc_page(stream, records, max_results=None):
    page = deepcopy({
        "CDCResponse": [{
            "QueryResponse": [{
                stream: records,
                "startPosition": 1,
                "maxResults": max_results if max_results is not None else len(records),
            }]
        }]
    })
    return page


def _deleted_bill(record_id, last_updated):
    return {
        "Id": str(record_id),
        "status": "Deleted",
        "MetaData": {"LastUpdatedTime": last_updated},
    }


@pytest.fixture
def rest():
    qb = MagicMock()
    qb.instance_url = "https://example.com/v3/company/1"
    qb._get_standard_headers.return_value = {"Authorization": "Bearer tok"}
    return Rest(qb)


class TestQueryCdcDeletesCap:
    def test_pages_when_more_than_1000_changes(self, rest, load_fixture):
        fixture = load_fixture("streams/cdc/cdc_over_cap.json")
        sample = fixture["sample_deleted_record"]
        assert fixture["CDCResponse"][0]["QueryResponse"][0]["maxResults"] == CDC_MAX_RESULTS

        page1 = [
            _deleted_bill(i, f"2026-08-01T00:{i // 60:02d}:{i % 60:02d}Z")
            for i in range(CDC_MAX_RESULTS)
        ]
        assert sample["status"] == "Deleted"
        assert "LastUpdatedTime" in sample["MetaData"]

        page2 = [
            _deleted_bill(1000 + i, f"2026-08-02T00:00:{i:02d}Z")
            for i in range(5)
        ]

        resp1, resp2 = MagicMock(), MagicMock()
        resp1.json.return_value = _cdc_page("Bill", page1, max_results=CDC_MAX_RESULTS)
        resp2.json.return_value = _cdc_page("Bill", page2, max_results=len(page2))
        rest.qb._make_request = MagicMock(side_effect=[resp1, resp2])

        records = list(rest.query_cdc_deletes("Bill", "2026-08-01T00:00:00Z"))

        assert len(records) == CDC_MAX_RESULTS + 5
        assert all(rec["Deleted"] is True for rec in records)
        assert rest.qb._make_request.call_count == 2

        first_since = rest.qb._make_request.call_args_list[0].kwargs["params"]["changedSince"]
        second_since = rest.qb._make_request.call_args_list[1].kwargs["params"]["changedSince"]
        assert second_since > first_since
