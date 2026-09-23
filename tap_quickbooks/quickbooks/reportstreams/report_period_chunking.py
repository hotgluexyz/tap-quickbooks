"""Date-range chunking for Reports API v2 column caps (overflow columns become "Other")."""

import calendar
import datetime
from datetime import date, timedelta
from typing import Dict, Iterator, Tuple

# v2 Reports API caps period columns to 200 per request, excess is rolled into ColTitle "Other".
MAX_REPORT_PERIOD_COLUMNS = 200
MAX_DAYS_PER_REQUEST = MAX_REPORT_PERIOD_COLUMNS
MAX_MONTHS_PER_REQUEST = MAX_REPORT_PERIOD_COLUMNS

PERIOD_ROLLUP_COLUMN = "Other"


def iter_day_chunks(
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


def iter_month_chunks(
    start_date: date,
    end_date: date,
    max_months: int = MAX_MONTHS_PER_REQUEST,
) -> Iterator[Tuple[date, date]]:
    """Yield inclusive date windows spanning at most max_months calendar months."""
    chunk_start = start_date
    while chunk_start <= end_date:
        month_index = (chunk_start.year * 12 + chunk_start.month - 1) + (max_months - 1)
        end_year = month_index // 12
        end_month = month_index % 12 + 1
        last_day = calendar.monthrange(end_year, end_month)[1]
        chunk_end = date(end_year, end_month, last_day)
        if chunk_end > end_date:
            chunk_end = end_date
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def record_merge_key(record: dict) -> tuple:
    """Stable merge key for account-level report rows."""
    return (record.get("Account"), tuple(record.get("Categories") or []))


def warn_if_period_rollup_present(
    logger,
    period_entries,
    *,
    report_name: str,
    period_start: str,
    period_end: str,
) -> None:
    """Log when QBO still returns the v2 overflow column despite chunking."""
    for entry in period_entries or []:
        if PERIOD_ROLLUP_COLUMN in entry:
            logger.warning(
                "%s response for %s to %s includes %s; period data may be incomplete",
                report_name,
                period_start,
                period_end,
                PERIOD_ROLLUP_COLUMN,
            )
            return


def merge_period_column_record(
    merged: Dict[tuple, dict],
    record: dict,
    *,
    period_attr: str,
    sum_total: bool = False,
) -> None:
    """Merge one parsed row into accumulated chunk results."""
    key = record_merge_key(record)
    period_values = list(record.get(period_attr) or [])
    if key not in merged:
        merged[key] = {k: v for k, v in record.items() if k != period_attr}
        merged[key][period_attr] = period_values
        return

    merged[key][period_attr].extend(period_values)
    if sum_total:
        merged[key]["Total"] += record["Total"]
