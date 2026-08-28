import datetime
from datetime import timedelta
from typing import ClassVar, Iterator, List, Tuple

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream

LOGGER = singer.get_logger()

# v2 caps summarize_column_by=Days at 200 daily columns per request; excess rolls into "Other".
MAX_DAYS_PER_REQUEST = 200


def _is_empty_or_zero_daily_value(value) -> bool:
    """True when a day column has no activity (v1 omits these; v2 often sends 0.00)."""
    if value == "":
        return True
    try:
        return float(str(value).replace(",", "")) == 0.0
    except (TypeError, ValueError):
        return False


def iter_date_chunks(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    max_days: int = MAX_DAYS_PER_REQUEST,
) -> Iterator[Tuple[datetime.datetime, datetime.datetime]]:
    """Yield inclusive (chunk_start, chunk_end) windows of at most max_days."""
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=max_days - 1), end_date)
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


class DailyCashFlowReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'DailyCashFlowReport'
    stream: ClassVar[str] = 'DailyCashFlowReport'
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

    def _parse_and_yield_rows(self, resp, columns):
        row_array = resp.get("Rows", {}).get("Row")
        if row_array is None:
            return

        output = []
        categories = []
        for row in row_array:
            self._recursive_row_search(row, output, categories)

        for raw_row in output:
            row = dict(zip(columns, raw_row))
            if not row.get("Total"):
                continue

            cleansed_row = {k: v for k, v in row.items() if v != ""}
            cleansed_row["Total"] = float(row.get("Total"))
            daily_total = [
                {key: value}
                for key, value in cleansed_row.items()
                if key not in ("Account", "Categories", "SyncTimestampUtc", "Total")
                and not _is_empty_or_zero_daily_value(value)
            ]
            cleansed_row["DailyTotal"] = daily_total
            yield cleansed_row

    def sync(self, catalog_entry):
        LOGGER.info("Starting full sync of CashFlow")
        current_date = datetime.datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=1)

        if self.qb.report_period_days:
            start_date = current_date - timedelta(days=int(self.qb.report_period_days))
        else:
            start_date = self.start_date.replace(tzinfo=None)

        merged = {}
        for chunk_start, chunk_end in iter_date_chunks(start_date, current_date):
            params = {
                "start_date": chunk_start.strftime("%Y-%m-%d"),
                "end_date": chunk_end.strftime("%Y-%m-%d"),
                "accounting_method": "Accrual",
                "summarize_column_by": "Days",
            }
            LOGGER.info(
                f"Fetch DailyCashFlow Report for period {params['start_date']} to {params['end_date']}"
            )
            resp = self._get(report_entity="CashFlow", params=params)
            columns = self._get_column_metadata(resp)
            for record in self._parse_and_yield_rows(resp, columns):
                key = (record.get("Account"), tuple(record.get("Categories") or []))
                if key not in merged:
                    merged[key] = record
                else:
                    merged[key]["DailyTotal"].extend(record["DailyTotal"])
                    merged[key]["Total"] += record["Total"]

        sync_ts = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
        for record in merged.values():
            record["SyncTimestampUtc"] = sync_ts
            yield record
