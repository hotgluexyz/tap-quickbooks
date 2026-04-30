import calendar
import datetime
from typing import Dict

import backoff
import requests
import singer

from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream, RetriableException, is_fatal_code

LOGGER = singer.get_logger()


def _is_fatal_including_504(e: requests.exceptions.RequestException) -> bool:
    """Fatal predicate for _get_504_fatal: same as is_fatal_code but also stops on 504.

    504 is treated as fatal so it propagates immediately to _process_period,
    which implements adaptive range-splitting instead of retrying the same
    oversized request.
    """
    return is_fatal_code(e) or e.response.status_code == 504

class BaseReportStream(QuickbooksStream):

    def __init__(
        self,
        qb,
        start_date,
        report_periods,
        state_passed=None,
    ):
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

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.HTTPError,
                          max_tries=10,
                          factor=3,
                          giveup=_is_fatal_including_504)
    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout,
                           RetriableException,
                           ),
                          max_tries=10,
                          factor=3)
    def _get_504_fatal(self, report_entity: str, params=None):
        """GET variant used by _process_period: 504 is fatal so it propagates
        immediately for adaptive range-splitting rather than being retried."""
        return self._execute_request(report_entity, params)
    
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

    def _process_period(self, report_entity, log_name, start_date, end_date, merged, track_total):
        """Fetch one date chunk and accumulate rows into merged.

        Single-month periods go directly to the point-in-time path, which avoids
        the summarize_column_by=Month parameter that causes 504s on dense data.
        Larger periods use the columnar path; on a 504 they are split in half and
        each half is retried recursively until reaching one month.
        """
        months = (
            (end_date.year - start_date.year) * 12
            + (end_date.month - start_date.month)
            + 1
        )

        if months == 1:
            self._process_period_point_in_time(report_entity, log_name, start_date, end_date, merged, track_total)
            return

        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual",
            "summarize_column_by": "Month",
        }
        LOGGER.info(f"Fetch {log_name} Report for period {params['start_date']} to {params['end_date']}")

        try:
            resp = self._get_504_fatal(report_entity=report_entity, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is None or e.response.status_code != 504:
                raise

            half = months // 2
            mid_year = start_date.year + (start_date.month - 1 + half) // 12
            mid_month = (start_date.month - 1 + half) % 12 + 1
            mid = datetime.date(mid_year, mid_month, 1)
            mid_end = mid - datetime.timedelta(days=1)

            LOGGER.warning(
                f"504 timeout for {log_name} {start_date} to {end_date} "
                f"({months} months) — splitting into "
                f"{start_date} to {mid_end} and {mid} to {end_date}"
            )
            self._process_period(report_entity, log_name, start_date, mid_end, merged, track_total)
            self._process_period(report_entity, log_name, mid, end_date, merged, track_total)
            return

        row_array = resp.get("Rows", {}).get("Row")
        if row_array is None:
            return

        columns = self._get_monthly_column_metadata(resp)
        output = []
        for row in row_array:
            self._recursive_row_search(row, output, [])
        for raw_row in output:
            self._merge_row_into_dict(raw_row, columns, merged, track_total)

    def _point_in_time_col_name(self, start_date, end_date):
        """Return the column name that matches what QBO would generate with summarize_column_by=Month.

        For a full calendar month (e.g. Feb 1-29): "Feb2024"
        For a partial month (e.g. Apr 1-29 when today is Apr 29): "Apr1-29,2026"
        The partial format replicates QBO's own ColTitle (e.g. "Apr 1-29, 2026") after
        stripping spaces, which is what _get_monthly_column_metadata already does.
        """
        last_day = calendar.monthrange(end_date.year, end_date.month)[1]
        if start_date.day == 1 and end_date.day == last_day:
            return end_date.strftime("%b%Y")
        return (
            f"{end_date.strftime('%b')} {start_date.day}-{end_date.day}, {end_date.year}"
        ).replace(" ", "")

    def _process_period_point_in_time(self, report_entity, log_name, start_date, end_date, merged, track_total):
        """Fallback fetch for a single-month period using a point-in-time request.

        Called when summarize_column_by=Month times out even at the minimum chunk size.
        Requests the report without summarize_column_by, which returns a single "Total"
        column (the balance/flow as of end_date). The month column label is chosen to
        match what QBO would have produced in a columnar response.
        """
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual",
        }
        LOGGER.info(f"Fetch {log_name} point-in-time for {params['start_date']} to {params['end_date']}")

        try:
            resp = self._get(report_entity=report_entity, params=params)
        except requests.exceptions.HTTPError:
            LOGGER.error(f"Point-in-time request failed for {log_name} {start_date} to {end_date}")
            raise

        row_array = resp.get("Rows", {}).get("Row")
        if row_array is None:
            return

        month_col = self._point_in_time_col_name(start_date, end_date)
        output = []
        for row in row_array:
            self._recursive_row_search(row, output, [])

        for raw_row in output:
            # raw_row is [account_value, total_value, categories_list]
            if len(raw_row) < 3:
                continue
            account, total_val, categories = raw_row[0], raw_row[1], raw_row[-1]
            if not total_val:
                continue
            # Cash flow: skip rows with no parent category (e.g. "Cash at beginning
            # of period"). These are balance items, not flows, and the columnar API
            # already excludes them by leaving their Total column empty.
            if track_total and not categories:
                continue

            key = (account, tuple(categories) if categories else ())
            if key not in merged:
                merged[key] = {
                    "Account": account,
                    "Categories": categories,
                    "MonthlyTotal": [],
                }
                if track_total:
                    merged[key]["Total"] = 0.0

            merged[key]["MonthlyTotal"].append({month_col: total_val})
            if track_total:
                try:
                    merged[key]["Total"] += float(total_val)
                except (ValueError, TypeError):
                    pass

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
            try:
                merged[key]["Total"] += float(row.get("Total"))
            except (ValueError, TypeError):
                pass
        merged[key]["MonthlyTotal"].extend(monthly_entries)

    def _sync_monthly_chunked(self, report_entity, log_name, track_total=False):
        """Syncs a monthly report using adaptive binary splitting.

        When track_total=True the Total value is summed separately and excluded from
        MonthlyTotal entries (cash flow semantics).

        Starts with the full date range in a single summarize_column_by=Month request.
        On 504, _process_period halves the range recursively. When a 1-month period
        still times out, _process_period_point_in_time fetches it without
        summarize_column_by so the request always completes. Column names produced by
        that fallback match what QBO would have generated in the columnar response.
        """
        LOGGER.info(f"Starting full sync of {log_name}")
        today = datetime.date.today()
        merged: Dict[tuple, dict] = {}

        self._process_period(report_entity, log_name, self.start_date.date(), today, merged, track_total)

        for record in merged.values():
            if not record["MonthlyTotal"]:
                continue
            if track_total:
                record["Total"] = round(record["Total"], 2)
            record["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
            yield record
