# pylint: disable=protected-access
import singer
import json
import singer.utils as singer_utils
from datetime import timedelta

from requests.exceptions import HTTPError
from tap_quickbooks.quickbooks.exceptions import TapQuickbooksException, raise_for_invalid_credentials

LOGGER = singer.get_logger()

MAX_RETRIES = 4
CDC_MAX_LOOKBACK_DAYS = 30
CDC_RESULT_LIMIT = 1000
CDC_PARTITION = timedelta(hours=4)
VENDOR_CREDIT_STREAM = "VendorCredit"

class Rest():

    def __init__(self, qb):
        self.qb = qb

    def query(self, catalog_entry, state):
        start_date = self.qb.get_start_date(state, catalog_entry)
        query = self.qb._build_query_string(catalog_entry, start_date)

        yield from self._query_recur(query, catalog_entry, start_date)

        if self.qb.include_deleted and catalog_entry['stream'] == VENDOR_CREDIT_STREAM:
            yield from self._sync_vendor_credit_cdc_deletes(start_date)

    # pylint: disable=too-many-arguments
    def _query_recur(
            self,
            query,
            catalog_entry,
            start_date_str,
            end_date=None,
            retries=MAX_RETRIES):
        params = {
            "query": query,
            "minorversion": "75"
        }
        url = f"{self.qb.instance_url}/query"
        headers = self.qb._get_standard_headers()

        sync_start = singer_utils.now()
        if end_date is None:
            end_date = sync_start

        if retries == 0:
            raise TapQuickbooksException(
                "Ran out of retries attempting to query Quickbooks Object {}".format(
                    catalog_entry['stream']))

        retryable = False
        try:
            for rec in self._sync_records(url, headers, params, catalog_entry['stream']):
                yield rec

            # If the date range was chunked (an end_date was passed), sync
            # from the end_date -> now
            if end_date < sync_start:
                next_start_date_str = singer_utils.strftime(end_date)
                query = self.qb._build_query_string(catalog_entry, next_start_date_str)
                for record in self._query_recur(
                        query,
                        catalog_entry,
                        next_start_date_str,
                        retries=retries):
                    yield record

        except HTTPError as ex:
            try:
                response = ex.response.json()
                if isinstance(response, list) and response[0].get("errorCode") == "QUERY_TIMEOUT":
                    start_date = singer_utils.strptime_with_tz(start_date_str)
                    day_range = (end_date - start_date).days
                    LOGGER.info(
                        "Quickbooks returned QUERY_TIMEOUT querying %d days of %s",
                        day_range,
                        catalog_entry['stream'])
                    retryable = True
                else:
                    raise_for_invalid_credentials(ex.response)
                    # raise any other unhandled exception
                    raise ex
            except TapQuickbooksException as qbe:
                raise qbe
            except:
                raise ex

        if retryable:
            start_date = singer_utils.strptime_with_tz(start_date_str)
            half_day_range = (end_date - start_date) // 2
            end_date = end_date - half_day_range

            if half_day_range.days == 0:
                raise TapQuickbooksException(
                    "Attempting to query by 0 day range, this would cause infinite looping.")

            query = self.qb._build_query_string(catalog_entry, singer_utils.strftime(start_date),
                                                singer_utils.strftime(end_date))
            for record in self._query_recur(
                    query,
                    catalog_entry,
                    start_date_str,
                    end_date,
                    retries - 1):
                yield record

    def _sync_records(self, url, headers, params, stream):
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"

        excluded_entities = ["Bill", "Payment", "Transfer", "CompanyInfo", "CreditMemo", "Invoice",
                            "JournalEntry", "Preferences", "Purchase", "SalesReceipt", "TimeActivity", "BillPayment","Estimate", "VendorCredit", "Attachable", "PurchaseOrder"]
        
        query = params['query']

        def sync_records(query, is_deleted=False):            
            offset = 0
            max = 100
            page = 0

            while True:
                headers.update(self.qb._get_standard_headers())
                params['query'] = f"{query}  STARTPOSITION {offset} MAXRESULTS {max}"
                resp = self.qb._make_request('GET', url, headers=headers, params=params)
                resp_json = resp.json()

                # Establish number of records returned.
                count = resp_json['QueryResponse'].get('maxResults', 0)

                # Make sure there is at least one record.
                if count == 0:
                    if is_deleted:
                        LOGGER.info(f"Response (deleted) with no data {resp_json}")
                    else:
                        LOGGER.info(f"Response with no data {resp_json}")
                    break;

                page += 1
                records = resp_json['QueryResponse'][stream];

                for _, rec in enumerate(records):
                    yield rec
                
                if count < max:
                    break;

                offset = (max * page) + 1
        

        # first fetch all active records
        yield from sync_records(query)

        # then fetch all deleted records
        if self.qb.include_deleted and stream not in excluded_entities:
            if "WHERE" in query:
                query_deleted = query.replace("WHERE", "where Active = false and")
            else:
                query_deleted = f"{query} where Active = false" 
            yield from sync_records(query_deleted, is_deleted=True)

    def _cdc_changed_since(self, start_date_str):
        changed_since = singer_utils.strptime_with_tz(start_date_str)
        earliest = singer_utils.now() - timedelta(days=CDC_MAX_LOOKBACK_DAYS)
        if changed_since < earliest:
            LOGGER.info(
                "Clamping VendorCredit CDC changedSince from %s to %s (QBO %d-day limit)",
                start_date_str,
                singer_utils.strftime(earliest),
                CDC_MAX_LOOKBACK_DAYS,
            )
            changed_since = earliest
        return singer_utils.strftime(changed_since)

    @staticmethod
    def _parse_metadata(rec):
        meta = rec.get('MetaData')
        if meta is None:
            return {}
        if isinstance(meta, dict):
            return meta
        if isinstance(meta, str):
            if not meta.strip():
                return {}
            try:
                parsed = json.loads(meta)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}
        return {}

    @staticmethod
    def _record_last_updated(rec):
        return Rest._parse_metadata(rec).get('LastUpdatedTime')

    @staticmethod
    def _record_in_cdc_window(rec, window_start, window_end):
        last_updated = Rest._record_last_updated(rec)
        if not last_updated:
            return True
        updated_at = singer_utils.strptime_with_tz(last_updated)
        return window_start <= updated_at < window_end

    def _fetch_cdc_vendor_credits(self, url, headers, changed_since):
        params = {
            "entities": VENDOR_CREDIT_STREAM,
            "changedSince": changed_since,
            "minorversion": "75",
        }
        resp = self.qb._make_request('GET', url, headers=headers, params=params)
        resp_json = resp.json()
        records = []
        truncated = False
        saw_page = False

        for query_page, vendor_credits in self._iter_cdc_vendor_credits(resp_json):
            saw_page = True
            records.extend(vendor_credits)
            total_count = query_page.get('totalCount') or len(vendor_credits)
            max_results = query_page.get('maxResults') or len(vendor_credits)
            if (
                len(vendor_credits) >= CDC_RESULT_LIMIT
                or total_count > max_results
            ):
                truncated = True

        if not saw_page:
            return [], False

        if len(records) >= CDC_RESULT_LIMIT:
            truncated = True
        return records, truncated

    def _yield_partition_cdc_deletes(
            self,
            url,
            headers,
            window_start,
            window_end,
            seen_deleted_ids):
        records, truncated = self._fetch_cdc_vendor_credits(
            url,
            headers,
            singer_utils.strftime(window_start),
        )

        for rec in records:
            if rec.get('status') != 'Deleted':
                continue
            if not self._record_in_cdc_window(rec, window_start, window_end):
                continue
            rec_id = rec.get('Id')
            if not rec_id or rec_id in seen_deleted_ids:
                continue
            seen_deleted_ids.add(rec_id)
            rec = dict(rec)
            rec['MetaData'] = Rest._parse_metadata(rec)
            yield rec

        if not truncated:
            return

        if records:
            boundary_last_updated = Rest._record_last_updated(records[-1])
            if boundary_last_updated:
                try:
                    if singer_utils.strptime_with_tz(boundary_last_updated) >= window_end:
                        return
                except (ValueError, TypeError):
                    pass

        window_span = window_end - window_start
        if window_span <= timedelta(seconds=1):
            raise TapQuickbooksException(
                "VendorCredit CDC sync incomplete: more than {} VendorCredit changes "
                "may exist between {} and {}.".format(
                    CDC_RESULT_LIMIT - 1,
                    singer_utils.strftime(window_start),
                    singer_utils.strftime(window_end),
                ))

        half = window_span // 2
        if half <= timedelta(0):
            raise TapQuickbooksException(
                "VendorCredit CDC sync incomplete: unable to subdivide window {} - {}.".format(
                    singer_utils.strftime(window_start),
                    singer_utils.strftime(window_end),
                ))

        midpoint = window_start + half
        yield from self._yield_partition_cdc_deletes(
            url, headers, window_start, midpoint, seen_deleted_ids)
        yield from self._yield_partition_cdc_deletes(
            url, headers, midpoint, window_end, seen_deleted_ids)

    @staticmethod
    def _iter_cdc_vendor_credits(cdc_payload):
        cdc_response = cdc_payload.get('CDCResponse') or []
        if isinstance(cdc_response, dict):
            cdc_response = [cdc_response]

        for cdc_block in cdc_response:
            query_response = cdc_block.get('QueryResponse') or {}
            if isinstance(query_response, list):
                for query_page in query_response:
                    vendor_credits = query_page.get(VENDOR_CREDIT_STREAM) or []
                    if not isinstance(vendor_credits, list):
                        vendor_credits = [vendor_credits]
                    yield query_page, vendor_credits
            else:
                vendor_credits = query_response.get(VENDOR_CREDIT_STREAM) or []
                if not isinstance(vendor_credits, list):
                    vendor_credits = [vendor_credits]
                yield query_response, vendor_credits

    def _sync_vendor_credit_cdc_deletes(self, start_date_str):
        url = f"{self.qb.instance_url}/cdc"
        headers = self.qb._get_standard_headers()
        headers["Accept"] = "application/json"

        window_start = singer_utils.strptime_with_tz(self._cdc_changed_since(start_date_str))
        window_end = singer_utils.now()
        seen_deleted_ids = set()

        partition_start = window_start
        try:
            while partition_start < window_end:
                partition_end = min(partition_start + CDC_PARTITION, window_end)
                yield from self._yield_partition_cdc_deletes(
                    url,
                    headers,
                    partition_start,
                    partition_end,
                    seen_deleted_ids,
                )
                partition_start = partition_end
        except HTTPError as ex:
            LOGGER.warning("VendorCredit CDC request failed: %s", ex)
            raise
        except Exception as ex:
            if ex.__class__.__name__ != "RetriableApiError":
                raise
            LOGGER.warning("VendorCredit CDC request failed: %s", ex)
            raise
