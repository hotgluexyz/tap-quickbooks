import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()


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

    def sync(self, catalog_entry):
        LOGGER.info(f"Starting full sync of MonthlyCashFlow")
        today = datetime.date.today()
        current_start = self.start_date.date()

        # Accumulate results across year-chunks, keyed by (Account, Categories).
        # Total is summed across chunks since it represents net cash flow over the period.
        merged: Dict[tuple, dict] = {}

        while current_start <= today:
            current_end = datetime.date(current_start.year, 12, 31)
            if current_end > today:
                current_end = today

            params = {
                "start_date": current_start.strftime("%Y-%m-%d"),
                "end_date": current_end.strftime("%Y-%m-%d"),
                "accounting_method": "Accrual",
                "summarize_column_by": "Month",
            }

            LOGGER.info(f"Fetch MonthlyCashFlow Report for period {params['start_date']} to {params['end_date']}")
            resp = self._get(report_entity='CashFlow', params=params)

            columns = self._get_column_metadata(resp)

            row_group = resp.get("Rows")
            row_array = row_group.get("Row")

            if row_array is None:
                current_start = datetime.date(current_start.year + 1, 1, 1)
                continue

            output = []
            categories = []
            for row in row_array:
                self._recursive_row_search(row, output, categories)

            for raw_row in output:
                row = dict(zip(columns, raw_row))
                if not row.get("Total"):
                    continue

                cleansed_row = {}
                for k, v in row.items():
                    if v == "":
                        continue
                    else:
                        cleansed_row[k] = v

                chunk_total = float(row.get("Total"))

                monthly_entries = []
                for key, value in cleansed_row.items():
                    if key not in ['Account', 'Categories', 'Total']:
                        monthly_entries.append({key: value})

                key = (cleansed_row.get("Account"), tuple(cleansed_row.get("Categories", [])))
                if key not in merged:
                    merged[key] = {
                        "Account": cleansed_row.get("Account"),
                        "Categories": cleansed_row.get("Categories"),
                        "Total": 0.0,
                        "MonthlyTotal": [],
                    }
                merged[key]["Total"] += chunk_total
                merged[key]["MonthlyTotal"].extend(monthly_entries)

            current_start = datetime.date(current_start.year + 1, 1, 1)

        for record in merged.values():
            if not record["MonthlyTotal"]:
                continue
            record["Total"] = round(record["Total"], 2)
            record["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
            yield record
 