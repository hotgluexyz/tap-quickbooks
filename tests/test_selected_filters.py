"""Unit tests for the Quickbooks client filter/query helpers added for
--selected-filters support (HGI-10451)."""

from unittest.mock import MagicMock

import pytest

from tap_quickbooks.quickbooks import Quickbooks
from tap_quickbooks.quickbooks.exceptions import TapQuickbooksException

pytestmark = pytest.mark.unit


@pytest.fixture
def qb():
    """A Quickbooks client that never logs in or hits the network."""
    return Quickbooks(
        api_type="REST",
        default_start_date="2000-01-01T00:00:00Z",
        realm_id="X",
    )


def bill_entry(replication_key="MetaData.LastUpdatedTime"):
    """Minimal catalog entry for the Bill stream."""
    stream_metadata = {"replication-key": replication_key} if replication_key else {}
    return {
        "stream": "Bill",
        "tap_stream_id": "Bill",
        "schema": {"properties": {}},
        "metadata": [{"breadcrumb": [], "metadata": stream_metadata}],
    }


class TestEscapeQuotes:
    def test_wraps_plain_string_in_single_quotes(self):
        assert Quickbooks._escape_quotes("abc") == "'abc'"

    def test_escapes_embedded_single_quote_with_backslash(self):
        # O'Brien -> 'O\'Brien'
        assert Quickbooks._escape_quotes("O'Brien") == "'O\\'Brien'"

    def test_non_string_returned_unchanged(self):
        assert Quickbooks._escape_quotes(56) == 56


class TestExtractId:
    def test_extracts_id_from_name_id_label(self):
        assert Quickbooks._extract_id("Alpha Co (56)") == "56"

    def test_uses_last_parenthesised_group_when_name_has_parens(self):
        assert Quickbooks._extract_id("LinkedIn Corporation  (CP) (56)") == "56"

    def test_plain_id_without_parens_returned_as_is(self):
        assert Quickbooks._extract_id("56") == "56"

    def test_trailing_whitespace_is_tolerated(self):
        assert Quickbooks._extract_id("Alpha Co (56)  ") == "56"

    def test_non_string_returned_unchanged(self):
        assert Quickbooks._extract_id(56) == 56


class TestParseClause:
    def test_in_list_extracts_ids_and_quotes(self, qb):
        clause = {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)", "Beta (57)"]}
        assert qb._parse_clause(clause) == "VendorRef IN ('56', '57')"

    def test_eq_uses_first_value(self, qb):
        clause = {"field": "DocNumber", "operator": "EQ", "value": "INV-1"}
        assert qb._parse_clause(clause) == "DocNumber = 'INV-1'"

    def test_numeric_values_are_not_quoted(self, qb):
        clause = {"field": "VendorRef", "operator": "IN", "value": [56, 57]}
        assert qb._parse_clause(clause) == "VendorRef IN (56, 57)"

    def test_empty_selection_returns_none(self, qb):
        clause = {"field": "VendorRef", "operator": "IN", "value": []}
        assert qb._parse_clause(clause) is None

    def test_unsupported_operator_raises(self, qb):
        clause = {"field": "VendorRef", "operator": "LIKE", "value": "x"}
        with pytest.raises(TapQuickbooksException):
            qb._parse_clause(clause)


class TestParseFilters:
    def test_flat_clause(self, qb):
        filters = {"clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]}}
        assert qb._parse_filters(filters) == ["VendorRef IN ('56')"]

    def test_clause_nested_under_group(self, qb):
        """Platform payloads nest clauses under group_* envelopes (HGI-10451 bug)."""
        filters = {"group_1": {"clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]}}}
        assert qb._parse_filters(filters) == ["VendorRef IN ('56')"]

    def test_deeply_nested_groups(self, qb):
        filters = {"group_1": {"group_2": {"clause_1": {"field": "VendorRef", "operator": "EQ", "value": "Alpha (56)"}}}}
        assert qb._parse_filters(filters) == ["VendorRef = '56'"]

    def test_multiple_clauses_in_group_collected(self, qb):
        filters = {
            "group_1": {
                "clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]},
                "operator_1": "AND",
                "clause_2": {"field": "DocNumber", "operator": "EQ", "value": "INV-1"},
            }
        }
        assert qb._parse_filters(filters) == ["VendorRef IN ('56')", "DocNumber = 'INV-1'"]

    def test_or_operator_raises(self, qb):
        filters = {
            "group_1": {
                "clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]},
                "operator_1": "OR",
                "clause_2": {"field": "VendorRef", "operator": "IN", "value": ["Beta (57)"]},
            }
        }
        with pytest.raises(TapQuickbooksException):
            qb._parse_filters(filters)

    def test_unknown_keys_ignored(self, qb):
        filters = {"foo": {"bar": 1}, "clause_1": {"field": "VendorRef", "operator": "EQ", "value": "56"}}
        assert qb._parse_filters(filters) == ["VendorRef = '56'"]


class TestGetSelectedFilterClause:
    def test_returns_none_when_no_filters_for_stream(self, qb):
        qb.selected_filters = {"streams": {"Invoice": {"clause_1": {}}}}
        assert qb._get_selected_filter_clause("Bill") is None

    def test_returns_none_when_no_selected_filters_at_all(self, qb):
        assert qb._get_selected_filter_clause("Bill") is None

    def test_joins_multiple_clauses_with_and(self, qb):
        qb.selected_filters = {
            "streams": {
                "Bill": {
                    "group_1": {
                        "clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]},
                        "clause_2": {"field": "DocNumber", "operator": "EQ", "value": "INV-1"},
                    }
                }
            }
        }
        assert qb._get_selected_filter_clause("Bill") == "VendorRef IN ('56') AND DocNumber = 'INV-1'"

    def test_empty_selection_yields_none(self, qb):
        qb.selected_filters = {"streams": {"Bill": {"clause_1": {"field": "VendorRef", "operator": "IN", "value": []}}}}
        assert qb._get_selected_filter_clause("Bill") is None


class TestBuildQueryString:
    def test_replication_key_only(self, qb):
        query = qb._build_query_string(bill_entry(), "2024-01-01T00:00:00Z")
        assert query == "SELECT * FROM Bill WHERE MetaData.LastUpdatedTime >  '2024-01-01T00:00:00Z'"

    def test_replication_key_and_selected_filter(self, qb):
        qb.selected_filters = {
            "streams": {"Bill": {"group_1": {"clause_1": {"field": "VendorRef", "operator": "IN", "value": ["Alpha (56)"]}}}}
        }
        query = qb._build_query_string(bill_entry(), "2024-01-01T00:00:00Z")
        assert query == (
            "SELECT * FROM Bill WHERE MetaData.LastUpdatedTime >  '2024-01-01T00:00:00Z' "
            "AND VendorRef IN ('56')"
        )

    def test_no_replication_key_and_no_filter_has_no_where(self, qb):
        query = qb._build_query_string(bill_entry(replication_key=None), "2024-01-01T00:00:00Z")
        assert query == "SELECT * FROM Bill"

    def test_end_date_adds_upper_bound(self, qb):
        query = qb._build_query_string(
            bill_entry(), "2024-01-01T00:00:00Z", end_date="2024-02-01T00:00:00Z"
        )
        assert "MetaData.LastUpdatedTime <= 2024-02-01T00:00:00Z" in query


class TestFetchAll:
    def test_single_page(self, qb):
        qb.access_token = "tok"
        resp = MagicMock()
        resp.json.return_value = {"QueryResponse": {"Vendor": [{"Id": "1"}, {"Id": "2"}]}}
        qb._make_request = MagicMock(return_value=resp)

        out = qb.fetch_all("Vendor", ["Id"])

        assert out == [{"Id": "1"}, {"Id": "2"}]
        assert qb._make_request.call_count == 1

    def test_empty_response(self, qb):
        qb.access_token = "tok"
        resp = MagicMock()
        resp.json.return_value = {"QueryResponse": {}}
        qb._make_request = MagicMock(return_value=resp)

        assert qb.fetch_all("Vendor", ["Id"]) == []
        assert qb._make_request.call_count == 1

    def test_paginates_until_short_page(self, qb):
        qb.access_token = "tok"
        page1 = [{"Id": str(i)} for i in range(1000)]
        page2 = [{"Id": "1000"}, {"Id": "1001"}]
        resp1, resp2 = MagicMock(), MagicMock()
        resp1.json.return_value = {"QueryResponse": {"Vendor": page1}}
        resp2.json.return_value = {"QueryResponse": {"Vendor": page2}}
        qb._make_request = MagicMock(side_effect=[resp1, resp2])

        out = qb.fetch_all("Vendor", ["Id"])

        assert len(out) == 1002
        assert qb._make_request.call_count == 2
        first_query = qb._make_request.call_args_list[0].kwargs["params"]["query"]
        second_query = qb._make_request.call_args_list[1].kwargs["params"]["query"]
        assert "STARTPOSITION 1 " in first_query
        assert "STARTPOSITION 1001 " in second_query
