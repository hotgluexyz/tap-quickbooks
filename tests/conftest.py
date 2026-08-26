"""Shared pytest fixtures."""

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def load_fixture():
    """Load JSON fixture files."""
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
def mock_qb():
    """Mock QuickBooks client with defaults for full sync."""
    qb = MagicMock()
    qb.gl_basic_fields = True
    qb.gl_full_sync = False
    qb.gl_daily = False
    qb.gl_weekly = False
    return qb
