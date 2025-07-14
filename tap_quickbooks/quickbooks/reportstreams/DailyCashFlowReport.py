import datetime
from datetime import timedelta
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.sync import transform_data_hook
from dateutil.relativedelta import relativedelta


LOGGER = singer.get_logger()


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
            values_copy = values.copy()
            output.append(values_copy)
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
    

    def check_date_greater_than_months(self,date_obj):
        # Get the current date
        today = datetime.datetime.now()

        # Calculate the difference in months between the date_obj and today
        months_difference = (today.year - date_obj.year) * 12 + (today.month - date_obj.month)

        # Check if the number of months is greater than 33
        return months_difference > 33
    
    def add_months(self, date_obj):
        months_qty=33
        # Calculate the new year and month after adding months
        new_date_obj = date_obj + relativedelta(months=+months_qty)
        return new_date_obj
    
    def correct_end_date(self, end_date, start_date, current_date):
        if end_date > current_date:
            # If end_date is greater than today then fetch report for yesterday.
            end_date = current_date - timedelta(days=1)
        if end_date <= start_date:
            end_date = start_date
        return end_date
    def sync(self, catalog_entry):
        full_sync = not self.state_passed

        if full_sync or self.qb.report_period_days:
            LOGGER.info(f"Starting full sync of CashFlow")
            current_date = datetime.datetime.now() - timedelta(days=1)
            #getting today's date as a datetime to avoid type errors
            min_time = datetime.datetime.min.time()
            today = datetime.date.today()
            today_datetime = datetime.datetime.combine(today, min_time)
            #-
            end_date = today_datetime + datetime.timedelta(60)
            if self.qb.report_period_days:
                start_date = today_datetime - datetime.timedelta(int(self.qb.report_period_days))
            else:
                start_date = self.start_date
            start_date = start_date.replace(tzinfo=None)

            if self.check_date_greater_than_months(start_date):
                end_date = self.add_months(start_date)
            end_date = self.correct_end_date(end_date,start_date,current_date)    
            params = {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "accounting_method": "Accrual",
                "summarize_column_by": "Days"
            }
            
            while start_date.replace(tzinfo=None) <= current_date:
                LOGGER.info(f"Fetch DailyCashFlow Report for period {params['start_date']} to {params['end_date']}")
                resp = self._get(report_entity='CashFlow', params=params)

                # Get column metadata.
                columns = self._get_column_metadata(resp)

                # Recursively get row data.
                row_group = resp.get("Rows")
                row_array = row_group.get("Row")

                if row_array is None:
                    return

                output = []
                categories = []
                for row in row_array:
                    self._recursive_row_search(row, output, categories)

                # Zip columns and row data.
                for raw_row in output:
                    row = dict(zip(columns, raw_row))
                    if not row.get("Total"):
                        # If a row is missing the amount, skip it
                        continue

                    cleansed_row = {}
                    for k, v in row.items():
                        if v == "":
                            continue
                        else:
                            cleansed_row.update({k: v})

                    cleansed_row["Total"] = float(row.get("Total"))
                    cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
                    daily_total = []
                    for key,value in cleansed_row.items():
                        if key not in ['Account', 'Categories', 'SyncTimestampUtc', 'Total']:
                            daily_total.append({key:value})
                    cleansed_row['DailyTotal'] = daily_total
                    start_date = end_date + timedelta(days=1)
                    end_date = self.add_months(start_date)
                    end_date = self.correct_end_date(end_date,start_date,current_date)  
                    yield cleansed_row
        else:
            LOGGER.info(f"Syncing CashFlow of last {self.number_of_periods} periods")
            end_date = datetime.date.today()

            for i in range(self.number_of_periods):
                start_date = end_date.replace(day=1)
                params = {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "accounting_method": "Accrual"
                }

                LOGGER.info(f"Fetch CashFlow for period {params['start_date']} to {params['end_date']}")
                resp = self._get(report_entity='CashFlow', params=params)

                # Get column metadata.
                columns = self._get_column_metadata(resp)

                # Recursively get row data.
                row_group = resp.get("Rows")
                row_array = row_group.get("Row")

                if row_array is None:
                    # Update end date
                    end_date = start_date - datetime.timedelta(days=1)
                    continue

                output = []
                categories = []
                for row in row_array:
                    self._recursive_row_search(row, output, categories)

                # Zip columns and row data.
                for raw_row in output:
                    row = dict(zip(columns, raw_row))
                    if not row.get("Total"):
                        # If a row is missing the amount, skip it
                        continue

                    cleansed_row = {}
                    for k, v in row.items():
                        if v == "":
                            continue
                        else:
                            cleansed_row.update({k: v})

                    cleansed_row["Total"] = float(row.get("Total"))
                    cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")

                    yield cleansed_row

                end_date = start_date - datetime.timedelta(days=1)
