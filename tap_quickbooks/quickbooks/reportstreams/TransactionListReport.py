import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream
from tap_quickbooks.sync import transform_data_hook

LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3

class TransactionListReport(QuickbooksStream):
    tap_stream_id: ClassVar[str] = 'TransactionListReport'
    stream: ClassVar[str] = 'TransactionListReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def __init__(self, qb, start_date, state_passed):
        self.qb = qb
        self.start_date = start_date
        self.state_passed = state_passed

    def _get_column_metadata(self, resp):
        columns = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColTitle") == "Memo/Description":
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
            values = [column for column in data]
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

    def sync(self, catalog_entry):
        full_sync = not self.state_passed

        if full_sync:
            LOGGER.info(f"Starting full sync of TransactionListReport")
            end_date = datetime.date.today()
            start_date = self.start_date
            cols = [
                'account_name',
                'create_by',
                'create_date',
                'cust_msg',
                'due_date',
                'doc_num',
                'inv_date',
                'is_ap_paid',
                'is_cleared',
                'is_no_post',
                'last_mod_by',
                'memo',
                'name',
                'other_account',
                'pmt_mthd',
                'printed',
                'sales_cust1',
                'sales_cust2',
                'sales_cust3',
                'term_name',
                'tracking_num',
                'tx_date',
                'txn_type',
                'term_name',
                'dept_name',
                'subt_nat_amount'
            ]
            params = {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "accounting_method": "Accrual",
                "columns": ",".join(cols)
            }

            LOGGER.info(f"Fetch TransactionListReport for period {params['start_date']} to {params['end_date']}")
            resp = self._get(report_entity='TransactionList', params=params)

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
                row = {}
                for c, v in zip(columns, raw_row):
                    if isinstance(v, dict):
                        row[c] = v.get("value")
                        if "id" in v:
                            row[f"{c}Id"] = v.get("id")
                    else:
                        row[c] = v

                if not row.get("Amount"):
                    # If a row is missing the amount, skip it
                    continue

                cleansed_row = {}
                for k, v in row.items():
                    if v == "":
                        continue
                    else:
                        cleansed_row.update({k: v})

                cleansed_row["Amount"] = float(row.get("Amount"))
                cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")

                yield cleansed_row
        else:
            LOGGER.info(f"Syncing TransactionListReport of last {NUMBER_OF_PERIODS} periods")
            end_date = datetime.date.today()

            for i in range(NUMBER_OF_PERIODS):
                start_date = end_date.replace(day=1)
                params = {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "accounting_method": "Accrual"
                }

                LOGGER.info(f"Fetch TransactionListReport for period {params['start_date']} to {params['end_date']}")
                resp = self._get(report_entity='TransactionList', params=params)

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
                    if not row.get("Amount"):
                        # If a row is missing the amount, skip it
                        continue

                    cleansed_row = {}
                    for k, v in row.items():
                        if v == "":
                            continue
                        else:
                            cleansed_row.update({k: v})
          
                    if type(row.get("Amount")) is dict:
                        cleansed_row['Amount'] = row["Amount"].get("value", 0)
                    else:
                        cleansed_row["Amount"] = float(row.get("Amount"))
            
                    cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")

                    yield cleansed_row

                end_date = start_date - datetime.timedelta(days=1)
