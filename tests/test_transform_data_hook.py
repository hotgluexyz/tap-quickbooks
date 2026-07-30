"""Tests for transform_data_hook string coercion."""

import json

from singer import Transformer

from tap_quickbooks.sync import transform_data_hook


def _bill_schema():
    string_type = {"type": ["string", "null"]}
    return {
        "type": "object",
        "properties": {
            "Line": string_type,
            "CurrencyRef": string_type,
            "VendorRef": string_type,
        },
    }


def test_stringifies_list_fields_as_json():
    record = {
        "Line": [
            {
                "Id": "1",
                "LineNum": 1,
                "Amount": 12.0,
                "DetailType": "ItemBasedExpenseLineDetail",
            }
        ],
        "CurrencyRef": {"value": "USD", "name": "United States Dollar"},
        "VendorRef": {"value": "146", "name": "BK Vendor 99900"},
    }

    with Transformer(pre_hook=transform_data_hook) as transformer:
        result = transformer.transform(record, _bill_schema())

    assert result["Line"].startswith('[{"')
    assert result["Line"] == json.dumps(record["Line"])
    assert result["CurrencyRef"] == json.dumps(record["CurrencyRef"])
    assert result["VendorRef"] == json.dumps(record["VendorRef"])


def test_does_not_stringify_lists_for_array_type():
    line = [{"Id": "1", "Amount": 5.0}]
    result = transform_data_hook(line, "array", {"type": ["array", "null"]})
    assert result == line
