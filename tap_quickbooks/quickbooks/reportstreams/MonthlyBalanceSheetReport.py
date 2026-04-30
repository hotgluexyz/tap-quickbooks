from typing import ClassVar, List

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream

LOGGER = singer.get_logger()


class MonthlyBalanceSheetReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'MonthlyBalanceSheetReport'
    stream: ClassVar[str] = 'MonthlyBalanceSheetReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def sync(self, catalog_entry):
        yield from self._sync_monthly_chunked(
            report_entity='BalanceSheet',
            log_name='MonthlyBalanceSheet',
            track_total=False,
        )
