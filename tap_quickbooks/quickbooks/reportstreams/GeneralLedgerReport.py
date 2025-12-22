import datetime
from typing import ClassVar, Dict, List, Optional

import singer

from tap_quickbooks.quickbooks.reportstreams.BaseReport import BaseReportStream
from tap_quickbooks.sync import transform_data_hook
from dateutil.relativedelta import relativedelta
import logging
import concurrent.futures
from calendar import monthrange
from tap_quickbooks.quickbooks.reportstreams.english_schemas.GeneralLedgerReportFields import glr_english_schema as eng_schema


LOGGER = singer.get_logger()


class GeneralLedgerReport(BaseReportStream):
    key_properties: ClassVar[List[str]] = []
    replication_method: ClassVar[str] = "FULL_TABLE"
    gl_weekly = False
    gl_daily = False

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

    def sync(self, catalog_entry):
        full_sync = not self.state_passed # and not self.has_number_of_periods

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
                        error_start_date = datetime.datetime.strptime(
                            r.get("start_date"), "%Y-%m-%d"
                        )
                        error_end_date = datetime.datetime.strptime(
                            r.get("end_date"), "%Y-%m-%d"
                        )
                        if not self.gl_weekly and not self.gl_daily:
                            self.gl_weekly = True
                            start_date = error_start_date
                        elif self.gl_weekly and not self.gl_daily:
                            self.gl_weekly = False
                            self.gl_daily = True
                            start_date = error_start_date
                        elif self.gl_daily:
                            # Set start_date to track which period we're processing
                            start_date = error_start_date
                            batch_size = 10
                            
                            # Define identity columns that will be included in every batch
                            # These are used to match rows across batches
                            identity_cols = ["tx_date", "txn_type", "subt_nat_amount", "subt_nat_home_amount", "subt_nat_amount_nt", "subt_nat_amount_home_nt", "credit_amt", "debt_amt", "credit_home_amt", "debt_home_amt"]
                            # Add doc_num and account_name if they exist in cols
                            if "doc_num" in cols:
                                identity_cols.append("doc_num")
                            if "account_name" in cols:
                                identity_cols.append("account_name")
                            
                            # Remove identity columns from cols to avoid duplication
                            # Keep original order for non-identity columns
                            other_cols = [c for c in cols if c not in identity_cols]
                            
                            # Create batches: each batch includes identity_cols + a slice of other_cols
                            column_batches = []
                            for i in range(0, len(other_cols), batch_size):
                                batch = identity_cols + other_cols[i:i+batch_size]
                                column_batches.append(batch)
                            
                            batch_params_list = []
                            for batch in column_batches:
                                batch_params = params.copy()
                                batch_params["columns"] = ",".join(batch)
                                batch_params["start_date"] = error_start_date.strftime("%Y-%m-%d")
                                batch_params["end_date"] = error_end_date.strftime("%Y-%m-%d")
                                batch_params_list.append(batch_params)
                            
                            with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_params_list)) as executor:
                                resp_batches = list(
                                    executor.map(
                                        lambda x: self.concurrent_get(report_entity="GeneralLedger", params=x),
                                        batch_params_list
                                    )
                                )
                            
                            # Dictionary to store rows by their identity key
                            # Key: tuple of identity column values, Value: list of row entries
                            # Using list to handle duplicate keys (pop in order)
                            rows_by_key = {}
                            key_order = []  # Maintain order of keys as they appear in first batch
                            
                            # Build complete column metadata list as we process batches
                            all_columns = []
                            identity_col_mappings = {}  # Map identity col name to its English schema name
                            
                            for id_col in identity_cols:
                                identity_col_mappings[id_col] = eng_schema.get(id_col, id_col)

                            for batch_idx, resp_batch in enumerate(resp_batches):
                                row_group = resp_batch.get("Rows")
                                row_array = row_group.get("Row")
                                
                                if row_array is None:
                                    continue
                                
                                # Get column metadata for this batch
                                batch_metadata = self._get_column_metadata(resp_batch, eng_schema)[:-1]  # Exclude Categories
                                
                                # Build complete column list as we process batches
                                if batch_idx == 0:
                                    all_columns = batch_metadata.copy()
                                else:
                                    # Add new columns from this batch to all_columns
                                    for col in batch_metadata:
                                        if col not in all_columns:
                                            all_columns.append(col)
                                
                                output = []
                                categories = []
                                for row in row_array:
                                    self._recursive_row_search(row, output, categories)
                                
                                # Find identity column indices in this batch's metadata
                                identity_indices = []
                                for id_col in identity_cols:
                                    id_col_mapped = identity_col_mappings[id_col]
                                    if id_col_mapped in batch_metadata:
                                        identity_indices.append(batch_metadata.index(id_col_mapped))
                                    else:
                                        identity_indices.append(None)
                                
                                # Process each row in this batch
                                for raw_row in output:
                                    # Extract identity key from raw_row
                                    identity_values = []
                                    for idx in identity_indices:
                                        if idx is not None and idx < len(raw_row) - 1:  # -1 for categories
                                            cell = raw_row[idx]
                                            # Extract value from dict or use directly
                                            if isinstance(cell, dict):
                                                identity_values.append(cell.get("value", ""))
                                            else:
                                                identity_values.append(cell if cell is not None else "")
                                        else:
                                            identity_values.append("")
                                    row_key = tuple(identity_values)
                                    
                                    # Extract all column data for this row, maintaining batch_metadata order
                                    row_data_by_col = {}
                                    for col_idx, col_name in enumerate(batch_metadata):
                                        if col_idx < len(raw_row) - 1:  # -1 for categories
                                            row_data_by_col[col_name] = raw_row[col_idx]
                                    
                                    categories_data = set(raw_row[-1]) if raw_row[-1] else set()
                                    
                                    # Store row data
                                    if batch_idx == 0:
                                        # First batch: initialize row entry
                                        if row_key not in rows_by_key:
                                            rows_by_key[row_key] = []
                                            key_order.append(row_key)
                                        
                                        rows_by_key[row_key].append({
                                            'column_data': row_data_by_col.copy(),
                                            'categories': categories_data,
                                            'batches_processed': [0]
                                        })
                                    else:
                                        # Subsequent batches: find matching row and merge data
                                        if row_key in rows_by_key and rows_by_key[row_key]:
                                            # Find the first unprocessed row with this key
                                            for row_entry in rows_by_key[row_key]:
                                                if batch_idx not in row_entry['batches_processed']:
                                                    # Merge column data from this batch
                                                    row_entry['column_data'].update(row_data_by_col)
                                                    row_entry['categories'].update(categories_data)
                                                    row_entry['batches_processed'].append(batch_idx)
                                                    break
                            
                            # Reconstruct stitched rows in key_order with correct column ordering
                            stitched_rows = []
                            row_categories = []
                            for row_key in key_order:
                                if row_key in rows_by_key:
                                    for row_entry in rows_by_key[row_key]:
                                        # Build row in all_columns order
                                        stitched_row = []
                                        for col in all_columns:
                                            if col in row_entry['column_data']:
                                                stitched_row.append(row_entry['column_data'][col])
                                            else:
                                                # Column not present in any batch for this row
                                                stitched_row.append(None)
                                        
                                        stitched_rows.append(stitched_row)
                                        row_categories.append(list(row_entry['categories']))
                            
                            columns_from_metadata = all_columns
                            
                            if stitched_rows:
                                # Join categories to the right of the rows
                                for i, row in enumerate(stitched_rows):
                                    row.append(row_categories[i])
                                
                                # Add the categories column at the end
                                columns_from_metadata.append("Categories")
                                
                                # We are ready to yield the full rows now
                                yield from self.clean_row(stitched_rows, columns_from_metadata)

                            # After successful batching, continue to process other responses
                            # The while loop will naturally continue after all responses are processed
                            start_date = error_end_date + datetime.timedelta(days=1)
                            break
                        else:
                            # If we already are at gl_daily we have to give up
                            raise Exception(r)

                        # For mode switching (weekly/daily), break to retry with new mode
                        # The start_date is already set to error_start_date above
                        break
                    else:
                        self.gl_weekly = False
                        self.gl_daily = False

                        # Get column metadata.
                        columns = self._get_column_metadata(r, eng_schema)

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
                f"Syncing GeneralLedgerReport of last {self.number_of_periods} periods"
            )
            end_date = datetime.date.today()

            for i in range(self.number_of_periods):
                start_date = end_date.replace(day=1)

                params["start_date"] = (end_date.replace(day=1).strftime("%Y-%m-%d"),)
                params["end_date"] = (end_date.strftime("%Y-%m-%d"),)

                LOGGER.info(
                    f"Fetch GeneralLedgerReport for period {params['start_date']} to {params['end_date']}"
                )
                resp = self._get(report_entity="GeneralLedger", params=params)

                # Get column metadata.
                columns = self._get_column_metadata(resp, eng_schema)

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
