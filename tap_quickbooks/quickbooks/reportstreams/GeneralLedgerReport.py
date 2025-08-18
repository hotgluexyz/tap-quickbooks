import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.rest_reports import QuickbooksStream
from tap_quickbooks.sync import transform_data_hook
from dateutil.relativedelta import relativedelta
import logging
import concurrent.futures
from calendar import monthrange


LOGGER = singer.get_logger()
NUMBER_OF_PERIODS = 3


class GeneralLedgerReport(QuickbooksStream):
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = "FULL_TABLE"
    gl_weekly = False
    gl_daily = False

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
        if "ColData" in list(row.keys()):
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

    def clean_row(self, output, columns):
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

            cleansed_row["SyncTimestampUtc"] = singer.utils.strftime(
                singer.utils.now(), "%Y-%m-%dT%H:%M:%SZ"
            )

            yield cleansed_row

    def concurrent_get(self, report_entity, params):
        log_msg = f"Fetch GeneralLedgerReport for period {params['start_date']} to {params['end_date']}"
        LOGGER.info(log_msg)
        response = self._get(report_entity, params)
        LOGGER.info(f"COMPLETE: {log_msg}")

        if "Unable to display more data. Please reduce the date range." in str(
            response
        ):
            return {
                "error": "Too much data for current period",
                "start_date": params["start_date"],
                "end_date": params["end_date"],
            }
        else:
            return response

    def sync(self, catalog_entry):
        full_sync = not self.state_passed

        if self.qb.gl_basic_fields:
            cols = [
                "tx_date",
                "subt_nat_amount",
                "credit_amt",
                "debt_amt",
                "subt_nat_home_amount",
                "credit_home_amt",
                "debt_home_amt",
                "account_name",
                "account_num",
                "klass_name",
                "dept_name",
                "item_name",
                "vend_name",
                "txn_type",
                "currency"
            ]
        else:
            cols = [
                "account_name",
                "chk_print_state",
                "create_by",
                "create_date",
                "cust_name",
                "doc_num",
                "emp_name",
                "inv_date",
                "is_adj",
                "is_ap_paid",
                "is_ar_paid",
                "is_cleared",
                "item_name",
                "last_mod_by",
                "last_mod_date",
                "memo",
                "name",
                "quantity",
                "rate",
                "split_acc",
                "tx_date",
                "txn_type",
                "vend_name",
                "net_amount",
                "tax_amount",
                "tax_code",
                "account_num",
                "klass_name",
                "dept_name",
                "debt_amt",
                "credit_amt",
                "nat_open_bal",
                "subt_nat_amount",
                "subt_nat_amount_nt",
                "debt_home_amt",
                "credit_home_amt",
                "currency",
                "exch_rate",
                "nat_home_open_bal",
                "nat_foreign_open_bal",
                "subt_nat_home_amount",
                "subt_nat_amount_home_nt",
            ]

        params = {
            "accounting_method": self.accounting_method,
            "columns": ",".join(cols),
            "sort_by": "tx_date"
        }

        if full_sync or self.qb.gl_full_sync:
            LOGGER.info(f"Starting full sync of GeneralLedgerReport")
            start_date = self.start_date
            start_date = start_date.replace(tzinfo=None)
            min_time = datetime.datetime.min.time()
            today = datetime.date.today()
            today = datetime.datetime.combine(today, min_time)

            # params for requests if self.concurrent_requests is true
            requests_params = []

            while start_date < today:
                # get the number of days and max number of requests
                if self.qb.gl_daily or self.gl_daily:
                    period_days = 1
                    max_requests = 10

                elif self.qb.gl_weekly or self.gl_weekly:
                    period_days = 7
                    max_requests = 10
                else:
                    day1, period_days = monthrange(start_date.year, start_date.month)
                    max_requests = 10

                # calculate end date
                if (today - start_date).days <= period_days:
                    end_date = today
                    params["end_date"] = today.strftime("%Y-%m-%d")
                else:
                    end_date = start_date + relativedelta(days=+period_days)
                    params["end_date"] = (
                        end_date - datetime.timedelta(days=1)
                    ).strftime("%Y-%m-%d")

                params["start_date"] = (start_date).strftime("%Y-%m-%d")
                requests_params.append(params.copy())

                # assign next start_date
                start_date = end_date

                # get the data
                if len(requests_params) < max_requests and end_date < today:
                    continue
                elif len(requests_params) == max_requests or end_date == today:
                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=max_requests
                    ) as executor:
                        resp = executor.map(
                            lambda x: self.concurrent_get(
                                report_entity="GeneralLedger", params=x
                            ),
                            requests_params,
                        )
                    requests_params = []
    
                # parse data and set the new start_date
                for r in resp:
                    if r.get("error") == "Too much data for current period":
                        start_date = datetime.datetime.strptime(
                            r.get("start_date"), "%Y-%m-%d"
                        )
                        if not self.gl_weekly and not self.gl_daily:
                            self.gl_weekly = True
                        elif self.gl_weekly and not self.gl_daily:
                            self.gl_weekly = False
                            self.gl_daily = True
                        elif self.gl_daily:
                            batch_size = 10
                            stitched_rows = []
                            row_categories = []
                            # Add tx_date to each batch to keep rows sorted
                            column_batches = [["tx_date"] + cols[i:i+batch_size] for i in range(0, len(cols), batch_size)]
                            batch_params_list = []
                            for batch in column_batches:
                                batch_params = params.copy()
                                batch_params["columns"] = ",".join(batch)
                                batch_params["start_date"] = start_date.strftime("%Y-%m-%d")
                                batch_params["end_date"] = end_date.strftime("%Y-%m-%d")
                                batch_params_list.append(batch_params)
                            with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_params_list)) as executor:
                                resp_batches = list(
                                    executor.map(
                                        lambda x: self.concurrent_get(report_entity="GeneralLedger", params=x),
                                        batch_params_list
                                    )
                                )
                            columns_from_metadata = ['Date']
                            for i, resp_batch in enumerate(resp_batches):
                                # remove tx_date and categories while appending to columns_from_metadata
                                # tx_date will be added automatically as it's already a column that will be fetched in a batch
                                # categories will be added in the end after all the columns are stitched together
                                columns_from_metadata += self._get_column_metadata(resp_batch)[1:-1]

                                row_group = resp_batch.get("Rows")
                                row_array = row_group.get("Row")

                                start_date = end_date
                                if row_array is None:
                                    continue

                                output = []
                                categories = []
                                for row in row_array:
                                    self._recursive_row_search(row, output, categories)

                                for i, raw_row in enumerate(output):
                                    # if the row was never inserted in stitched_rows, append it
                                    if len(stitched_rows) <= i:
                                        stitched_rows.append(raw_row[:-1])
                                        # row_categories maintains a set of categories to avoid duplication
                                        row_categories.append({*raw_row[-1]})
                                    # if the row was already inserted, join new columns to the right
                                    else:
                                        stitched_rows[i] += raw_row[1:-1]
                                        row_categories[i].update(raw_row[-1])

                            if stitched_rows:
                                # join categories to the right of the rows
                                for i, row in enumerate(stitched_rows):
                                    row += [list(row_categories[i])]

                                # add the categories column at the end
                                columns_from_metadata.append("Categories")

                                # we are ready to yield the full rows now
                                yield from self.clean_row(stitched_rows, columns_from_metadata)
                            break
                        else:
                            # If we already are at gl_daily we have to give up
                            raise Exception(r)

                        break
                    else:
                        self.gl_weekly = False
                        self.gl_daily = False

                        # Get column metadata.
                        columns = self._get_column_metadata(r)

                        # Recursively get row data.
                        row_group = r.get("Rows")
                        row_array = row_group.get("Row")

                        start_date = end_date
                        if row_array is None:
                            continue

                        output = []
                        categories = []
                        for row in row_array:
                            self._recursive_row_search(row, output, categories)

                        yield from self.clean_row(output, columns)
        else:
            LOGGER.info(
                f"Syncing GeneralLedgerReport of last {NUMBER_OF_PERIODS} periods"
            )
            end_date = datetime.date.today()

            for i in range(NUMBER_OF_PERIODS):
                start_date = end_date.replace(day=1)

                params["start_date"] = (end_date.replace(day=1).strftime("%Y-%m-%d"),)
                params["end_date"] = (end_date.strftime("%Y-%m-%d"),)

                LOGGER.info(
                    f"Fetch GeneralLedgerReport for period {params['start_date']} to {params['end_date']}"
                )
                resp = self._get(report_entity="GeneralLedger", params=params)

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

                yield from self.clean_row(output, columns)

                end_date = start_date - datetime.timedelta(days=1)
