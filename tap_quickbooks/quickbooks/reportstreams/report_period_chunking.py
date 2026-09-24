"""Month-bounded windows for Reports API v2 (overflow period columns become "Other")."""

import calendar
from datetime import date, timedelta
from typing import Iterator, Tuple

# v2 caps summarize_column_by=Month at 200 columns per request.
MAX_MONTHS_PER_REQUEST = 200


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
