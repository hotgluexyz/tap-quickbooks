"""Tests for secret masking helpers."""

from tap_quickbooks.util import mask_secret, redact_for_log


def test_mask_secret_shows_suffix():
    assert mask_secret("test-refresh-token-abcdef") == "***cdef"


def test_mask_secret_handles_short_values():
    assert mask_secret("abc") == "***"


def test_redact_for_log_masks_oauth_body():
    body = {
        "grant_type": "refresh_token",
        "client_id": "test-client-id-1234567890",
        "client_secret": "test-client-secret-abcdefghij",
        "refresh_token": "test-refresh-token-abcdef",
    }

    redacted = redact_for_log(body)

    assert redacted["client_id"] == body["client_id"]
    assert redacted["client_secret"] == "***ghij"
    assert redacted["refresh_token"] == "***cdef"
    assert body["client_secret"] == "test-client-secret-abcdefghij"


def test_redact_for_log_passes_through_non_mappings():
    assert redact_for_log("plain") == "plain"
