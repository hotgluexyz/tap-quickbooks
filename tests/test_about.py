from unittest.mock import patch

from tap_quickbooks import QuickbooksTap


EXPECTED_STREAMS = [
    "Account",
    "Bill",
    "BillPayment",
    "CreditMemo",
    "Customer",
    "Employee",
    "Estimate",
    "Invoice",
    "Item",
    "JournalEntry",
    "Payment",
    "Purchase",
    "PurchaseOrder",
    "SalesReceipt",
    "TimeActivity",
    "Transfer",
    "Vendor",
    "VendorCredit",
]


def test_about_lists_packaged_objects_without_authenticated_discovery():
    tap = QuickbooksTap(config={}, validate_config=False)

    with patch.object(
        tap,
        "_build_qb",
        side_effect=AssertionError("about must not build an authenticated client"),
    ) as build_qb:
        about = tap._get_about_info()

    build_qb.assert_not_called()
    assert about["supported_streams"] == EXPECTED_STREAMS
