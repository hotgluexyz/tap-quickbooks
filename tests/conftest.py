"""Shared pytest fixtures."""

import datetime
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from tap_quickbooks.quickbooks.reportstreams.GeneralLedgerAccrualReport import (
    GeneralLedgerAccrualReport,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

@pytest.fixture
def load_fixture():
    """Load JSON fixture files.
    
    Usage:
        load_fixture("streams/report/basic_response")
        load_fixture("common/empty_responses")
    """
    def _load(name: str):
        if not name.endswith(".json"):
            name = f"{name}.json"
        
        fixture_path = FIXTURES_DIR / name
        
        if not fixture_path.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture_path}")
        
        with open(fixture_path) as f:
            return json.load(f)
    return _load

@pytest.fixture
def load_report_fixture(load_fixture):
    """Load fixtures from streams/report/ directory."""
    def _load(name: str):
        return load_fixture(f"streams/report/{name}")
    return _load


@pytest.fixture
def sample_data():
    """Provide sample test data."""
    return {"key": "value", "count": 42}


@pytest.fixture
def mock_client():
    """Create a mock API client."""
    client = MagicMock()
    client.get.return_value = {"status": "ok"}
    return client


# Report stream fixtures (data from tests/fixtures/streams/report/*.json)

@pytest.fixture
def mock_qb():
    """Mock QuickBooks client with defaults for full sync."""
    qb = MagicMock()
    qb.gl_basic_fields = True
    qb.gl_full_sync = False
    qb.gl_daily = False
    qb.gl_weekly = False
    return qb


@pytest.fixture
def start_date():
    """Start date = first day of current month so one period reaches today (single request)."""
    today = datetime.date.today()
    return datetime.datetime.combine(today.replace(day=1), datetime.time.min)


@pytest.fixture
def catalog_entry(load_report_fixture):
    """Minimal catalog entry for GeneralLedgerAccrualReport (from JSON)."""
    return load_report_fixture("catalog_entry.json")


@pytest.fixture
def report(mock_qb, start_date):
    """GeneralLedgerAccrualReport with state_passed=False so full_sync path is taken."""
    return GeneralLedgerAccrualReport(
        qb=mock_qb,
        start_date=start_date,
        report_periods=None,
        state_passed=False,
    )


@pytest.fixture
def gl_report_empty_rows(load_report_fixture):
    """GL report response with Rows.Row = null (from JSON)."""
    return load_report_fixture("gl_report_empty_rows.json")


@pytest.fixture
def gl_report_no_amount(load_report_fixture):
    """GL report response with empty amount in first row (from JSON)."""
    return load_report_fixture("gl_report_no_amount.json")
