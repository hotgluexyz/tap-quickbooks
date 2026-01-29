# pylint: disable=protected-access
import singer
import re
import singer.utils as singer_utils
from datetime import timedelta

from requests.exceptions import HTTPError
from tap_quickbooks.quickbooks.exceptions import TapQuickbooksException, raise_for_invalid_credentials

LOGGER = singer.get_logger()

MAX_RETRIES = 4

# Entities that support the CDC endpoint for delete tracking
# Note: CDC cannot be used for journalCode, taxAgency, timeActivity, taxCode, or taxRate
CDC_SUPPORTED_ENTITIES = [
    "Account", "Bill", "BillPayment", "Budget", "Class",
    "CreditMemo", "Customer", "Department", "Deposit", "Employee",
    "Estimate", "Invoice", "Item", "JournalEntry", "Payment",
    "PaymentMethod", "Purchase", "PurchaseOrder",
    "SalesReceipt", "Term", "Transfer", "Vendor",
    "VendorCredit", "CustomerType", "Attachable"
]

# Entities that have an Active field for soft-delete querying (fallback)
ENTITIES_WITH_ACTIVE_FIELD = [
    "Account", "Class", "CustomerType", "PaymentMethod", "TaxRate",
    "TaxCode", "Term", "Vendor", "Customer", "Employee", "Item",
    "Department"
]

# Entities excluded from delete syncing entirely
EXCLUDED_FROM_DELETE_SYNC = [
    "CompanyInfo", "Preferences"  # These don't support deletion
]

class Rest():

    def __init__(self, qb):
        self.qb = qb

    def query(self, catalog_entry, state):
        start_date = self.qb.get_start_date(state, catalog_entry)
        query = self.qb._build_query_string(catalog_entry, start_date)

        return self._query_recur(query, catalog_entry, start_date)

    def query_cdc_deletes(self, stream, changed_since):
        """
        Query the CDC endpoint to get deleted records for a specific entity.
        
        The CDC endpoint returns records with status="Deleted" for truly deleted items.
        CDC has a 30-day lookback limit.
        
        Args:
            stream: The entity type to query (e.g., "Invoice", "Customer")
            changed_since: ISO format datetime string for the changedSince parameter
        
        Yields:
            Records that have been deleted, with Deleted metadata added
        """
        if stream not in CDC_SUPPORTED_ENTITIES:
            LOGGER.info(f"CDC not supported for {stream}, skipping delete detection")
            return
        
        if stream in EXCLUDED_FROM_DELETE_SYNC:
            LOGGER.info(f"{stream} does not support deletion, skipping")
            return

        # CDC has a 30-day limit - adjust start date if needed
        changed_since_dt = singer_utils.strptime_with_tz(changed_since)
        thirty_days_ago = singer_utils.now() - timedelta(days=30)
        
        if changed_since_dt < thirty_days_ago:
            LOGGER.info(f"CDC has 30-day limit. Adjusting changed_since from {changed_since} to 30 days ago")
            changed_since = singer_utils.strftime(thirty_days_ago)

        url = f"{self.qb.instance_url}/cdc"
        headers = self.qb._get_standard_headers()
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/text"
        
        params = {
            "entities": stream,
            "changedSince": changed_since,
            "minorversion": "75"
        }
        
        LOGGER.info(f"Querying CDC for deleted {stream} records since {changed_since}")
        
        try:
            resp = self.qb._make_request('GET', url, headers=headers, params=params)
            resp_json = resp.json()
            
            cdc_response = resp_json.get('CDCResponse', [])
            if not cdc_response:
                LOGGER.info(f"No CDC response for {stream}")
                return
            
            for cdc_item in cdc_response:
                query_response = cdc_item.get('QueryResponse', [])
                for qr in query_response:
                    records = qr.get(stream, [])
                    for rec in records:
                        # Check if this is a deleted record
                        if rec.get('status') == 'Deleted':
                            # Add deletion metadata
                            rec['Deleted'] = True
                            LOGGER.info(f"Found deleted {stream} record: Id={rec.get('Id')}")
                            yield rec
                            
        except HTTPError as ex:
            LOGGER.warning(f"CDC query failed for {stream}: {ex}")
            # Don't fail the entire sync if CDC fails
            return
        except Exception as ex:
            LOGGER.warning(f"Unexpected error querying CDC for {stream}: {ex}")
            return

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
                    break

                page += 1
                records = resp_json['QueryResponse'][stream]

                for rec in records:
                    if is_deleted:
                        # Mark soft-deleted/inactive records
                        rec['Deleted'] = True
                    yield rec
                
                if count < max:
                    break

                offset = (max * page) + 1
        

        # First fetch all active records
        yield from sync_records(query)

        # Handle deleted records based on configuration
        if self.qb.include_deleted and stream not in EXCLUDED_FROM_DELETE_SYNC:
            # Method 1: Use CDC endpoint for true delete detection (preferred)
            if stream in CDC_SUPPORTED_ENTITIES:
                # Extract the start date from the WHERE clause or use default
                start_date = self.qb.default_start_date
                # Try to extract date from query
                where_match = re.search(r"WHERE.*?>\s*'([^']+)'", query, re.IGNORECASE)
                if where_match:
                    start_date = where_match.group(1)
                
                LOGGER.info(f"Using CDC to detect deleted {stream} records since {start_date}")
                yield from self.query_cdc_deletes(stream, start_date)
            
            # Method 2: Fallback to Active=false query for soft deletes
            elif stream in ENTITIES_WITH_ACTIVE_FIELD:
                LOGGER.info(f"Using Active=false query for soft-deleted {stream} records")
                if "WHERE" in query:
                    query_deleted = query.replace("WHERE", "where Active = false and")
                else:
                    query_deleted = f"{query} where Active = false" 
                yield from sync_records(query_deleted, is_deleted=True)
            else:
                LOGGER.info(f"No delete detection method available for {stream}")
