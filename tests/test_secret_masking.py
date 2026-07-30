"""Tests for secret masking helpers."""

from tap_quickbooks.util import mask_secret, redact_for_log


def test_mask_secret_shows_suffix():
    assert mask_secret("RT1-126-H0-1794170450b2w4wb69ey4diwpfxvcf") == "***xvcf"


def test_mask_secret_handles_short_values():
    assert mask_secret("abc") == "***"


def test_redact_for_log_masks_oauth_body():
    body = {
        "grant_type": "refresh_token",
        "client_id": "ABELs0U8bWJw6ZH4rNZS6y2Wai6qQlVUTtzQoS32OhW0x6AccX",
        "client_secret": "LucG0PEhvuS1zyZT04Gwg0AuFARTD2kKfEsnoxdl",
        "refresh_token": "RT1-126-H0-1794170450b2w4wb69ey4diwpfxvcf",
    }

    redacted = redact_for_log(body)

    assert redacted["client_id"] == body["client_id"]
    assert redacted["client_secret"] == "***oxdl"
    assert redacted["refresh_token"] == "***xvcf"
    assert body["client_secret"] == "LucG0PEhvuS1zyZT04Gwg0AuFARTD2kKfEsnoxdl"


def test_redact_for_log_passes_through_non_mappings():
    assert redact_for_log("plain") == "plain"
