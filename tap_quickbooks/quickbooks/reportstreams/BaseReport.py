from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream

class BaseReportStream(QuickbooksStream):
    
    def __init__(self, qb, start_date, report_periods, state_passed):
        self.qb = qb
        self.start_date = start_date
        self.has_number_of_periods = report_periods is not None
        self.number_of_periods = report_periods or 3
        self.state_passed = state_passed