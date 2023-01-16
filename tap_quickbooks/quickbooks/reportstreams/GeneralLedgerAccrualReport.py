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
    
    def __init__(self, qb, start_date, state_passed):
        self.qb = qb
        self.start_date = start_date
        self.state_passed = state_passed
        self.accounting_method = "Accrual"

