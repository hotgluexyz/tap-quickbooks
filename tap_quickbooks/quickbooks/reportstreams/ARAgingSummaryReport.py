import datetime
from typing import ClassVar, Dict, List, Optional

import singer
from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()


class ARAgingSummaryReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'ARAgingSummaryReport'
    stream: ClassVar[str] = 'ARAgingSummaryReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def _get_column_metadata(self, resp):
        columns = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColTitle") == "" and column.get("ColType") == "Customer":
                columns.append("Customer")
            else:
                columns.append(column.get("ColTitle").replace(" ", ""))
        return columns

    def sync(self, catalog_entry):
        LOGGER.info(f"Starting full sync of ARAgingSummary")
        end_date = datetime.date.today()
        start_date = self.start_date
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual"
        }

        LOGGER.info(f"Fetch ARAgingSummary Report for period {params['start_date']} to {params['end_date']}")
        resp = self._get(report_entity='AgedReceivables', params=params)

        # Get column metadata.
        columns = self._get_column_metadata(resp)

        # Recursively get row data.
        row_group = resp.get("Rows")
        row_array = row_group.get("Row")

        if row_array is None:
            return

        output = []
        for row in row_array:
            if "Header" in row:
                output.append([i.get('value') for i in row.get("Header", {}).get("ColData", [])])

                for subrow in row.get("Rows", {}).get("Row", []):
                    output.append([i.get('value') for i in subrow.get("ColData", [])])

                output.append([i.get('value') for i in row.get("Summary", {}).get("ColData", [])])
            elif "Summary" in row:
                output.append([i.get('value') for i in row.get("Summary", {}).get("ColData", [])])
            else:
                output.append([i.get('value') for i in row.get("ColData", [])])

        # Zip columns and row data.
        for raw_row in output:
            row = dict(zip(columns, raw_row))
            if not row.get("Total"):
                # If a row is missing the amount, skip it
                continue

            cleansed_row = {}
            for k, v in row.items():
                if v == "":
                    continue
                else:
                    cleansed_row.update({k: v})

            cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
            cleansed_row["StartDate"] = start_date.strftime("%Y-%m-%d")
            cleansed_row["EndDate"] = end_date.strftime("%Y-%m-%d")

            yield cleansed_row
