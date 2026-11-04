"""Tests for secret masking helpers."""

from unittest.mock import MagicMock

import tap_quickbooks.util as util
from tap_quickbooks.util import mask_secret, redact_for_log, save_api_usage

OAUTH_BODY = {
    "grant_type": "refresh_token",
    "client_id": "test-client-id-1234567890",
    "client_secret": "test-client-secret-abcdefghij",
    "refresh_token": "test-refresh-token-abcdef",
}


def test_mask_secret_shows_suffix():
    assert mask_secret("test-refresh-token-abcdef") == "***cdef"


def test_mask_secret_handles_short_values():
    assert mask_secret("abc") == "***"


def test_redact_for_log_masks_oauth_body():
    body = dict(OAUTH_BODY)

    redacted = redact_for_log(body)

    assert redacted["client_id"] == body["client_id"]
    assert redacted["client_secret"] == "***ghij"
    assert redacted["refresh_token"] == "***cdef"
    assert body["client_secret"] == "test-client-secret-abcdefghij"


def test_redact_for_log_passes_through_non_mappings():
    assert redact_for_log("plain") == "plain"
    assert redact_for_log(None) is None


def test_save_api_usage_redacts_oauth_body(monkeypatch):
    captured = []

    class FakeQueue:
        def put(self, entry):
            captured.append(entry)

    monkeypatch.setattr(util, "_log_queue", FakeQueue())
    monkeypatch.setattr(util, "_ensure_log_thread", lambda: None)

    body = dict(OAUTH_BODY)
    response = MagicMock(status_code=200)

    save_api_usage(
        "POST",
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        None,
        body,
        response,
    )

    assert len(captured) == 1
    logged_body = captured[0]["request"]["body"]
    assert logged_body["client_secret"] == "***ghij"
    assert logged_body["refresh_token"] == "***cdef"
    assert body["client_secret"] == "test-client-secret-abcdefghij"
