import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream
from tap_quickbooks.sync import transform_data_hook
from dateutil.parser import parse

LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3

class ProfitAndLossDetailReport(QuickbooksStream):
    tap_stream_id: ClassVar[str] = 'ProfitAndLossDetailReport'
    stream: ClassVar[str] = 'ProfitAndLossDetailReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'
    current_account = {}

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
        if row.get("type")=="Section":
            if row.get("Header", {}).get("ColData", [{}]):
                if row.get("Header", {}).get("ColData", [{}])[0].get("id"):
                    self.current_account = row.get("Header", {}).get("ColData", [{}])[0]
        if 'ColData' in list(row.keys()):
            # Write the row
            data = row.get("ColData")
            values = [column for column in data]
            categories_copy = categories.copy()
            values.append(categories_copy)
            values_copy = values.copy()
            if values_copy:
                values_copy += [self.current_account]
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

        cols = [
            "create_by",
            "create_date",
            "doc_num",
            "last_mod_by",
            "last_mod_date",
            "memo",
            "name",
            "pmt_mthd",
            "split_acc",
            "tx_date",
            "txn_type",
            "tax_code",
            "klass_name",
            "dept_name",
            "debt_amt",
            "debt_home_amt",
            "credit_amt",
            "credit_home_amt",
            "currency",
            "exch_rate",
            "nat_open_bal",
            "nat_home_open_bal",
            "nat_foreign_open_bal",
            "subt_nat_amount",
            "subt_nat_home_amount",
            "subt_nat_amount_nt",
            "subt_nat_amount_home_nt",
            "rbal_nat_amount",
            "rbal_nat_home_amount",
            "rbal_nat_amount_nt",
            "rbal_nat_amount_home_nt",
            "tax_amount",
            "home_tax_amount",
            "net_amount",
            "home_net_amount",
        ]

        if full_sync:
            start_date = self.start_date.date()
            delta = 364

            while start_date<datetime.date.today():
                LOGGER.info(f"Starting full sync of P&L")
                end_date = (start_date + datetime.timedelta(delta))
                if end_date>datetime.date.today():
                    end_date = datetime.date.today()

                params = {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "accounting_method": "Accrual",
                    "columns": ",".join(cols)
                }

                LOGGER.info(f"Fetch Journal Report for period {params['start_date']} to {params['end_date']}")
                resp = self._get(report_entity='ProfitAndLossDetail', params=params)
                start_date = end_date + datetime.timedelta(1)

                # Get column metadata.
                columns = self._get_column_metadata(resp)
                columns += ["Account"]

                # Recursively get row data.
                row_group = resp.get("Rows")
                row_array = row_group.get("Row")

                if row_array is None:
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
                        if isinstance(v, dict):
                            cleansed_row[k] = v.get("value")
                            if "id" in v:
                                cleansed_row[f"{k}Id"] = v.get("id")
                        else:
                            cleansed_row[k] = v

                    cleansed_row["Amount"] = float(cleansed_row.get("Amount")) if cleansed_row.get("Amount") else None
                    cleansed_row["Balance"] = float(cleansed_row.get("Balance")) if cleansed_row.get("Amount") else None
                    cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
                    if cleansed_row.get('Date'):
                        try:
                            cleansed_row["Date"] = parse(cleansed_row['Date'])
                        except:
                            continue

                    yield cleansed_row
        else:
            LOGGER.info(f"Syncing P&L of last {NUMBER_OF_PERIODS} periods")
            end_date = datetime.date.today()

            for i in range(NUMBER_OF_PERIODS):
                start_date = end_date.replace(day=1)
                params = {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "accounting_method": "Accrual",
                    "columns": ",".join(cols)
                }

                LOGGER.info(f"Fetch Journal Report for period {params['start_date']} to {params['end_date']}")
                resp = self._get(report_entity='ProfitAndLossDetail', params=params)

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
                        if isinstance(v, dict):
                            cleansed_row[k] = v.get("value")
                            if "id" in v:
                                cleansed_row[f"{k}Id"] = v.get("id")
                        else:
                            cleansed_row[k] = v

                    cleansed_row["Amount"] = float(cleansed_row.get("Amount")) if cleansed_row.get("Amount") else None
                    cleansed_row["Balance"] = float(cleansed_row.get("Balance")) if cleansed_row.get("Amount") else None
                    cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ")
                    if cleansed_row.get('Date'):
                        cleansed_row["Date"] = parse(cleansed_row['Date'])

                    yield cleansed_row

                end_date = start_date - datetime.timedelta(days=1)
