import datetime
from typing import ClassVar, Dict, List, Optional
import singer
from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3

class ARAgingDetailReport(QuickbooksStream):
    tap_stream_id: ClassVar[str] = 'ARAgingDetailReport'
    stream: ClassVar[str] = 'ARAgingDetailReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def __init__(self, qb, start_date, state_passed):
        self.qb = qb
        self.start_date = start_date
        self.state_passed = state_passed

    def _get_column_metadata(self, resp):
        columns = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColTitle") == "" and column.get("ColType") == "Customer":
                columns.append("Customer")
            else:
                columns.append(column.get("ColTitle").replace(" ", ""))
        return columns

    def sync(self, catalog_entry):
        LOGGER.info(f"Starting full sync of ARAgingDetail")
        end_date = datetime.date.today()
        start_date = self.start_date
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual"
        }

        report_dates = ['2024-05-31', '2024-06-30']
        if self.qb.ar_aging_report_dates:
            for report_date in self.qb.ar_aging_report_dates:
                report_dates.append(report_date.split("T")[0])
        elif self.qb.ar_aging_report_date:
            report_dates.append(self.qb.ar_aging_report_date.split("T")[0])
        else:
            report_dates.append(None) # This is to Run the sync once without specific report_date

        for report_date in report_dates:
            if report_date:
                params["aging_method"] = "Report_Date"
                params["report_date"] = report_date
                LOGGER.info(f"Fetch ARAgingDetail Report for period {params['start_date']} to {params['end_date']} with aging_method 'Report_Date' and report_date {report_date}")
            else:
                LOGGER.info(f"Fetch ARAgingDetail Report for period {params['start_date']} to {params['end_date']}")
            resp = self._get(report_entity='AgedReceivableDetail', params=params)

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
                row["report_date"] = report_date if report_date else end_date.strftime("%Y-%m-%d")
       
                cleansed_row = {}
                for k, v in row.items():
                    if v == "":
                        continue
                    else:
                        cleansed_row.update({k: v})
                cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
                yield cleansed_row     
