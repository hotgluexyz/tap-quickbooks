"""Unit tests for QuickbooksTap.get_available_filters / _load_reference_data
added for --get-available-filters support (HGI-10451)."""

import json
from unittest.mock import MagicMock

import pytest

from tap_quickbooks import QuickbooksTap
from tap_quickbooks.quickbooks.exceptions import TapQuickbooksException

pytestmark = pytest.mark.unit


@pytest.fixture
def tap():
    config = {
        "refresh_token": "x",
        "client_id": "x",
        "client_secret": "x",
        "start_date": "2000-01-01T00:00:00Z",
        "realmId": "X",
        "select_fields_by_default": True,
    }
    return QuickbooksTap(config=config, validate_config=False)


def catalog_with(stream_name, selected=True):
    return {
        "streams": [
            {
                "stream": stream_name,
                "tap_stream_id": stream_name,
                "metadata": [{"breadcrumb": [], "metadata": {"selected": selected}}],
            }
        ]
    }


class TestLoadReferenceData:
    def test_maps_qb_fields_to_logical_and_adds_label(self, tap):
        qb = MagicMock()
        qb.fetch_all.return_value = [
            {"Id": "56", "DisplayName": "Acme (West)"},
            {"Id": "57", "DisplayName": "Beta"},
        ]

        out = tap._load_reference_data(qb, {"Vendor": {"name(id)"}})

        qb.fetch_all.assert_called_once_with("Vendor", ["Id", "DisplayName"])
        assert out["Vendor"][0] == {"id": "56", "name": "Acme (West)", "name(id)": "Acme (West) (56)"}
        assert out["Vendor"][1] == {"id": "57", "name": "Beta", "name(id)": "Beta (57)"}

    def test_unknown_reference_stream_raises(self, tap):
        with pytest.raises(TapQuickbooksException):
            tap._load_reference_data(MagicMock(), {"Customer": {"id"}})


class TestGetAvailableFilters:
    def test_emits_payload_with_reference_data(self, tap, capsys):
        fake_qb = MagicMock()
        fake_qb.fetch_all.return_value = [{"Id": "56", "DisplayName": "Acme"}]
        tap._build_qb = lambda: fake_qb
        tap._qb_cleanup = lambda qb: None

        tap.get_available_filters(catalog_with("Bill"))

        payload = json.loads(capsys.readouterr().out)
        assert payload["filters_version"] == "1.0.0"
        assert "Bill" in payload["streams"]
        assert payload["streams"]["Bill"]["filters"]["vendors"]["target_field"] == "VendorRef"
        assert payload["reference_data"]["Vendor"][0]["name(id)"] == "Acme (56)"

    def test_no_filterable_stream_skips_reference_fetch(self, tap, capsys):
        build_calls = {"n": 0}

        def build():
            build_calls["n"] += 1
            return MagicMock()

        tap._build_qb = build
        tap._qb_cleanup = lambda qb: None

        tap.get_available_filters(catalog_with("Account"))

        payload = json.loads(capsys.readouterr().out)
        assert payload["streams"] == {}
        assert payload["reference_data"] == {}
        assert build_calls["n"] == 0

    def test_accepts_catalog_dict_directly(self, tap, capsys):
        fake_qb = MagicMock()
        fake_qb.fetch_all.return_value = []
        tap._build_qb = lambda: fake_qb
        tap._qb_cleanup = lambda qb: None

        tap.get_available_filters(catalog_with("Bill"))

        payload = json.loads(capsys.readouterr().out)
        assert payload["reference_data"]["Vendor"] == []
