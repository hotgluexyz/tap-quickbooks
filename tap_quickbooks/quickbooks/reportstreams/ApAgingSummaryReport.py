import datetime
from typing import ClassVar, Dict, List, Optional
import singer
from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()

class ApAgingSummaryReport(QuickbooksStream):
    """Stream class to sync the Accounts Payable Aging Summary Report from QuickBooks."""
    
    tap_stream_id: ClassVar[str] = 'APAgingSummaryReport'
    stream: ClassVar[str] = 'APAgingSummaryReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def __init__(self, qb, start_date, state_passed):
        """Initialize the APAgingSummaryReport stream.
        
        Args:
            qb: QuickBooks client instance.
            start_date: The start date for the report data.
            state_passed: State information passed to the stream.
        """
        self.qb = qb
        self.start_date = start_date
        self.state_passed = state_passed

    def _get_column_metadata(self, resp):
        """Extract column names from the report response.
        
        Args:
            resp: The API response containing report data.
            
        Returns:
            List of column names.
        """
        columns = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColTitle") == "" and column.get("ColType") == "Vendor":
                columns.append("Vendor")
            else:
                columns.append(column.get("ColTitle").replace(" ", ""))
        return columns

    def sync(self, catalog_entry):
        """Sync the APAgingSummaryReport data from QuickBooks.
        
        Fetches the report for the specified period, processes rows and columns,
        and yields transformed records.
        
        Args:
            catalog_entry: Catalog entry for the stream (not used in this implementation).
            
        Yields:
            Dictionaries representing individual report rows with vendor aging data.
        """
        LOGGER.info("Starting full sync of APAgingSummary")
        end_date = datetime.date.today()
        start_date = self.start_date
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual"
        }

        # Determine report dates to fetch
        report_dates = []
        if self.qb.ar_aging_report_dates:
            for report_date in self.qb.ar_aging_report_dates:
                report_dates.append(report_date.split("T")[0])
        elif self.qb.ar_aging_report_date:
            report_dates.append(self.qb.ar_aging_report_date.split("T")[0])
        else:
            report_dates.append(None)  # Run once without a specific report date
        LOGGER.info(f"Fetch APAgingSummary with report_dates {report_dates}")
        for report_date in report_dates:
            if report_date:
                params["aging_method"] = "Report_Date"
                params["report_date"] = report_date
                LOGGER.info(f"Fetch APAgingSummary Report for period {params['start_date']} to {params['end_date']} with aging_method 'Report_Date' and report_date {report_date}")
            else:
                LOGGER.info(f"Fetch APAgingSummary Report for period {params['start_date']} to {params['end_date']}")

            # Fetch the report using the 'AgedPayables' endpoint
            resp = self._get(report_entity='AgedPayables', params=params)

            # Get column metadata
            columns = self._get_column_metadata(resp)

            # Extract row data
            row_group = resp.get("Rows")
            row_array = row_group.get("Row")

            if row_array is None:
                LOGGER.info(f"No APAgingSummary Report found for period {params['start_date']} to {params['end_date']} with aging_method 'Report_Date' and report_date {report_date}")
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

            # Transform rows into dictionaries and yield
            for raw_row in output:
                row = dict(zip(columns, raw_row))
                row["report_date"] = report_date if report_date else end_date.strftime("%Y-%m-%d")
                if not row.get("Total"):
                    # Skip rows without a 'Total' value (e.g., separators)
                    continue
                
                # Cleanse the row by removing empty values
                cleansed_row = {}
                for k, v in row.items():
                    if v == "":
                        continue
                    else:
                        cleansed_row.update({k: v})
                
                # Add sync timestamp
                cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
                yield cleansed_row
