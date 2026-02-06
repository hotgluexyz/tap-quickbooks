"""Shared pytest fixtures."""

import pytest

@pytest.fixture
def sample_data():
    """Provide sample test data."""
    return {"key": "value", "count": 42}

@pytest.fixture
def mock_client():
    """Create a mock API client."""
    from unittest.mock import MagicMock
    client = MagicMock()
    client.get.return_value = {"status": "ok"}
    return client
