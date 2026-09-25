from unittest.mock import patch

from tap_quickbooks import QuickbooksTap
from tap_quickbooks.quickbooks import QB_OBJECTS


def test_about_lists_packaged_objects_without_authenticated_discovery():
    tap = QuickbooksTap(config={}, validate_config=False)

    with patch.object(tap, "_build_qb") as build_qb:
        about = tap._get_about_info()

    build_qb.assert_not_called()
    assert about["supported_streams"] == sorted(QB_OBJECTS)
