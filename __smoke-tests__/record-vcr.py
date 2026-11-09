import json

import vcr

from hotglue_smoke_test.vcr.sanitize import redact_credential
from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class QuickbooksTapTestRunner(VCRTapTestRunner):
    TOKEN_KEYS = [
        *VCRTapTestRunner.TOKEN_KEYS,
        "api_key",
        "expires_in",
        "realmId",
        "redirect_uri",
    ]
    PRESERVE_KEYS = {
        "Id",
        "MetaData.LastUpdatedTime",
        "LastUpdatedTime",
        "maxResults",
        "startPosition",
        "totalCount",
    }

    def __init__(self, test_case, script_dir):
        super().__init__(test_case, script_dir)
        config = json.loads(open(f"{self.test_case_path}/config.json").read())
        self.realm_id = config["realmId"]

    def scrub_uri(self, uri):
        return uri.replace(self.realm_id, redact_credential(self.realm_id))

    def vcr_use_cassette(self, filter_query_parameters):
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )

    def module(self):
        return "tap_quickbooks"

    def launch(self):
        from tap_quickbooks import main

        main()


if __name__ == "__main__":
    QuickbooksTapTestRunner.main()
