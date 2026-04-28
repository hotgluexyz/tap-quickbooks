from typing import ClassVar, List

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()


class MonthlyCashFlowReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'MonthlyCashFlowReport'
    stream: ClassVar[str] = 'MonthlyCashFlowReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def sync(self, catalog_entry):
        yield from self._sync_monthly_chunked(
            report_entity='CashFlow',
            log_name='MonthlyCashFlow',
            track_total=True,
        )
