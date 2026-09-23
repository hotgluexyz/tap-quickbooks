import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.quickbooks.reportstreams.report_period_chunking import (
    iter_month_chunks,
    merge_period_column_record,
    warn_if_period_rollup_present,
)
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3

class MonthlyCashFlowReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'MonthlyCashFlowReport'
    stream: ClassVar[str] = 'MonthlyCashFlowReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def _get_column_metadata(self, resp):
        columns = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColTitle") == "" and column.get("ColType") == "Account":
                columns.append("Account")
            elif column.get("ColTitle") == "Memo/Description":
                columns.append("Memo")
            else:
                columns.append(column.get("ColTitle").replace(" ", ""))
        columns.append("Categories")
        return columns

    def _recursive_row_search(self, row, output, categories):
        row_group = row.get("Rows")
        if 'ColData' in list(row.keys()):
            # Write the row
            data = row.get("ColData")
            values = [column.get("value") for column in data]
            categories_copy = categories.copy()
            values.append(categories_copy)
            values_copy = values.copy()
            output.append(values_copy)
        elif row_group is None or row_group == {}:
            pass
        else:
            row_array = row_group.get("Row")
            header = row.get("Header")
            if header is not None:
                categories.append(header.get("ColData")[0].get("value"))
            for row in row_array:
                self._recursive_row_search(row, output, categories)
            if header is not None:
                categories.pop()

    def _records_from_response(self, resp):
        """Parse one columnar CashFlow API response into records with MonthlyTotal."""
        # Get column metadata.
        columns = self._get_column_metadata(resp)

        # Recursively get row data.
        row_group = resp.get("Rows")
        row_array = row_group.get("Row")

        if row_array is None:
            return

        output = []
        categories = []
        for row in row_array:
            self._recursive_row_search(row, output, categories)

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

            cleansed_row["Total"] = float(row.get("Total"))
            monthly_total = []
            for key,value in cleansed_row.items():
                if key not in ['Account', 'Categories', 'SyncTimestampUtc', 'Total']:
                    monthly_total.append({key:value})
            cleansed_row['MonthlyTotal'] = monthly_total

            yield cleansed_row

    def sync(self, catalog_entry):
        """Full sync with month-bounded API requests merged by account and categories."""
        LOGGER.info(f"Starting full sync of MonthlyCashFlow")
        end_date = datetime.date.today()
        start_date = self.start_date
        if isinstance(start_date, datetime.datetime):
            start_date = start_date.date()

        merged = {}
        for chunk_start, chunk_end in iter_month_chunks(start_date, end_date):
            params = {
                "start_date": chunk_start.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
                "accounting_method": "Accrual",
                "summarize_column_by": "Month"
            }

            LOGGER.info(f"Fetch MonthlyCashFlow Report for period {params['start_date']} to {params['end_date']}")
            resp = self._get(report_entity='CashFlow', params=params)
            for record in self._records_from_response(resp):
                warn_if_period_rollup_present(
                    LOGGER,
                    record.get('MonthlyTotal'),
                    report_name="MonthlyCashFlow",
                    period_start=params["start_date"],
                    period_end=params["end_date"],
                )
                merge_period_column_record(
                    merged,
                    record,
                    period_attr='MonthlyTotal',
                    sum_total=True,
                )

        sync_ts = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
        for record in merged.values():
            record["SyncTimestampUtc"] = sync_ts
            yield record
