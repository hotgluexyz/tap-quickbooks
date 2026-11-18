from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class QuickbooksTapTestRunner(VCRTapTestRunner):
    TOKEN_KEYS = [
        *VCRTapTestRunner.TOKEN_KEYS,
        "expires_in",
        "redirect_uri",
    ]
    PRESERVE_KEYS = {
        "Id",
        "LastUpdatedTime",
        "ColTitle",
        "ColType",
        "type",
        "maxResults",
        "startPosition",
        "totalCount",
    }

    def module(self):
        return "tap_quickbooks"

    def launch(self):
        from tap_quickbooks import main

        main()


if __name__ == "__main__":
    QuickbooksTapTestRunner.main()
