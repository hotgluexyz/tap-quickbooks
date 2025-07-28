import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.GeneralLedgerReport import GeneralLedgerReport
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()


class GeneralLedgerCashReport(GeneralLedgerReport):
    tap_stream_id: ClassVar[str] = 'GeneralLedgerCashReport'
    stream: ClassVar[str] = 'GeneralLedgerCashReport'
    
    accounting_method = "Cash"