"""Report stream fixture data loaders."""

import copy
import json
import os

FIXTURES_DIR = os.path.join(os.path.dirname(__file__))


def load_fixture(name):
    """Load a JSON fixture from tests/fixtures/streams/report/."""
    path = os.path.join(FIXTURES_DIR, name)
    with open(path, "r") as f:
        return json.load(f)


def minimal_gl_report_response(start_date_str, amount="100"):
    """Load gl_report_response.json and override date/amount in the first row."""
    data = load_fixture("gl_report_response.json")
    data = copy.deepcopy(data)
    data["Rows"]["Row"][0]["ColData"][0]["value"] = start_date_str
    data["Rows"]["Row"][0]["ColData"][1]["value"] = amount
    return data
