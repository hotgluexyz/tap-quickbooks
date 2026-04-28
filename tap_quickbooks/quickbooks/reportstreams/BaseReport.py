import datetime
from typing import Dict

import singer

from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream

LOGGER = singer.get_logger()

class BaseReportStream(QuickbooksStream):
    
    def __init__(self, qb, start_date, report_periods, state_passed=None):
        self.qb = qb
        self.start_date = start_date
        self.has_number_of_periods = report_periods is not None
        self.number_of_periods = report_periods or 3
        self.state_passed = state_passed
    
    def concurrent_get(self, report_entity, params):
        log_msg = f"Fetch {report_entity} for period {params['start_date']} to {params['end_date']}"
        LOGGER.info(log_msg)
        response = self._get(report_entity, params)
        LOGGER.info(f"COMPLETE: {log_msg}")

        if "Unable to display more data. Please reduce the date range." in str(
            response
        ):
            return {
                "error": "Too much data for current period",
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            }
        else:
            return response
    
    def _get_column_metadata(self, resp, schema=None):
        columns = []
        for column in resp.get("Columns").get("Column"):
            # To handle multiple languages if schema is passed, always convert Col Titles to english
            if schema is not None:
                col_type = column["MetaData"][0].get("Value") if column.get("MetaData") else None
                if not col_type:
                    LOGGER.info(f"Metadata for col {column.get('ColTitle')} not found, skipping.")
                    continue
                # append col to columns
                col_title = column.get("ColTitle")
                if col_title in schema.values():
                    columns.append(col_title.replace(" ", ""))
                else:
                    columns.append(schema.get(col_type))
            else:
                if column.get("ColTitle") == "Memo/Description":
                    columns.append("Memo")
                else:
                    columns.append(column.get("ColTitle").replace(" ", ""))
        columns.append("Categories")
        return columns

    def _get_monthly_column_metadata(self, resp):
        """Column parser for monthly reports: maps empty Account-type cols to 'Account'."""
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
            data = row.get("ColData")
            values = [column.get("value") for column in data]
            categories_copy = categories.copy()
            values.append(categories_copy)
            output.append(values.copy())
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

    def _fetch_chunk_rows(self, report_entity, log_name, start_date, end_date):
        """Fetch one year chunk from the QBO API and return (columns, flat_rows), or None if empty."""
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual",
            "summarize_column_by": "Month",
        }
        LOGGER.info(f"Fetch {log_name} Report for period {params['start_date']} to {params['end_date']}")
        resp = self._get(report_entity=report_entity, params=params)

        row_array = resp.get("Rows", {}).get("Row")
        if row_array is None:
            return None

        columns = self._get_monthly_column_metadata(resp)
        output = []
        for row in row_array:
            self._recursive_row_search(row, output, [])
        return columns, output

    def _merge_row_into_dict(self, raw_row, columns, merged, track_total):
        """Accumulate one raw row into the cross-chunk merged dict."""
        row = dict(zip(columns, raw_row))

        if track_total and not row.get("Total"):
            return

        cleansed_row = {k: v for k, v in row.items() if v != ""}
        exclude_keys = {"Account", "Categories", "Total"} if track_total else {"Account", "Categories"}
        monthly_entries = [{k: v} for k, v in cleansed_row.items() if k not in exclude_keys]

        if not track_total and not monthly_entries:
            return

        key = (cleansed_row.get("Account"), tuple(cleansed_row.get("Categories", [])))
        if key not in merged:
            merged[key] = {
                "Account": cleansed_row.get("Account"),
                "Categories": cleansed_row.get("Categories"),
                "MonthlyTotal": [],
            }
            if track_total:
                merged[key]["Total"] = 0.0

        if track_total:
            merged[key]["Total"] += float(row.get("Total"))
        merged[key]["MonthlyTotal"].extend(monthly_entries)

    def _sync_monthly_chunked(self, report_entity, log_name, track_total=False):
        """Year-chunked sync shared by MonthlyBalanceSheet and MonthlyCashFlow.

        When track_total=True the Total column is excluded from MonthlyTotal entries
        and summed separately across chunks (cash flow semantics).
        """
        LOGGER.info(f"Starting full sync of {log_name}")
        today = datetime.date.today()
        current_start = self.start_date.date()
        merged: Dict[tuple, dict] = {}

        while current_start <= today:
            current_end = min(datetime.date(current_start.year, 12, 31), today)
            chunk = self._fetch_chunk_rows(report_entity, log_name, current_start, current_end)
            if chunk is not None:
                columns, output = chunk
                for raw_row in output:
                    self._merge_row_into_dict(raw_row, columns, merged, track_total)
            current_start = datetime.date(current_start.year + 1, 1, 1)

        for record in merged.values():
            if not record["MonthlyTotal"]:
                continue
            if track_total:
                record["Total"] = round(record["Total"], 2)
            record["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
            yield record
