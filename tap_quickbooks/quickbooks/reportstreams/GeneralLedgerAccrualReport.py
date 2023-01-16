import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.GeneralLedgerReport import GeneralLedgerReport
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3

class GeneralLedgerAccrualReport(GeneralLedgerReport):
    tap_stream_id: ClassVar[str] = 'GeneralLedgerAccrualReport'
    stream: ClassVar[str] = 'GeneralLedgerAccrualReport'
    
    accounting_method = "Accrual"

