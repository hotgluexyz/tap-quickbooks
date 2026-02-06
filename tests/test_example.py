"""Tests for example module."""

import pytest

class TestExample:
    """Test cases for example functionality."""

    def test_basic_functionality(self, sample_data):
        """Test basic functionality with fixture."""
        assert sample_data["key"] == "value"
        assert sample_data["count"] == 42

    def test_with_mock(self, mock_client):
        """Test with mocked client."""
        result = mock_client.get()
        assert result["status"] == "ok"

    @pytest.mark.unit
    def test_marked_as_unit(self):
        """Test marked with custom marker."""
        assert True
