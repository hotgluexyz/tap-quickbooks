import datetime
from typing import ClassVar, List

import backoff
import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.quickbooks.rest_reports import RetriableException

LOGGER = singer.get_logger()


class TransactionListReport(BaseReportStream):
    tap_stream_id: ClassVar[str] = 'TransactionListReport'
    stream: ClassVar[str] = 'TransactionListReport'
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = 'FULL_TABLE'

    def _validate_transaction_list_response(self, resp):
        """Raise on Intuit Fault; retry when the report payload has no column metadata."""
        fault = resp.get("Fault")
        if fault:
            errors = fault.get("Error") or [{}]
            err = errors[0] if errors else {}
            message = err.get("Message") or "QuickBooks report error"
            detail = err.get("Detail") or err.get("code") or ""
            raise Exception(f"QuickBooks TransactionList error: {message} ({detail})")

        if not resp.get("Columns") or resp["Columns"].get("Column") is None:
            LOGGER.warning(
                "TransactionList response missing Columns; will retry. Top-level keys: %s",
                list(resp.keys()),
            )
            raise RetriableException("TransactionList response missing Columns")

    @backoff.on_exception(
        backoff.expo,
        RetriableException,
        max_tries=5,
        factor=3,
    )
    def _fetch_transaction_list(self, params):
        """GET TransactionList with retries when QBO returns an unusable 200 body."""
        resp = self._execute_request("TransactionList", params)
        self._validate_transaction_list_response(resp)
        return resp

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
            'dept_name',
            # Request both amounts; QBO returns only the one applicable for the company.
            'subt_nat_amount',
            'subt_nat_home_amount',
        ]
        params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "accounting_method": "Accrual",
            "columns": ",".join(cols)
        }

        LOGGER.info(f"Fetch TransactionListReport for period {params['start_date']} to {params['end_date']}")
        resp = self._fetch_transaction_list(params)

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
