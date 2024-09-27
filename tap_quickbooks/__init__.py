#!/usr/bin/env python3
import json
import sys
import singer
import singer.utils as singer_utils
from singer import metadata, metrics
import tap_quickbooks.quickbooks as quickbooks
from tap_quickbooks.sync import (sync_stream, get_stream_version)
from tap_quickbooks.quickbooks import Quickbooks
from tap_quickbooks.quickbooks.exceptions import (
    TapQuickbooksException, TapQuickbooksQuotaExceededException)

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
                        'refresh_token',
                        'client_id',
                        'client_secret',
                        'start_date',
                        'realmId',
                        'select_fields_by_default'
                        ]

CONFIG = {
    'refresh_token': None,
    'client_id': None,
    'client_secret': None,
    'start_date': None,
    'include_deleted': None
}

REPLICATION_KEY="MetaData.LastUpdatedTime"

def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)

def build_state(raw_state, catalog):
    state = {}

    for catalog_entry in catalog['streams']:
        tap_stream_id = catalog_entry['tap_stream_id']
        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_method = catalog_metadata.get((), {}).get('replication-method')

        version = singer.get_bookmark(raw_state,
                                      tap_stream_id,
                                      'version')

        # Preserve state that deals with resuming an incomplete bulk job
        if singer.get_bookmark(raw_state, tap_stream_id, 'JobID'):
            job_id = singer.get_bookmark(raw_state, tap_stream_id, 'JobID')
            batches = singer.get_bookmark(raw_state, tap_stream_id, 'BatchIDs')
            current_bookmark = singer.get_bookmark(raw_state, tap_stream_id, 'JobHighestBookmarkSeen')
            state = singer.write_bookmark(state, tap_stream_id, 'JobID', job_id)
            state = singer.write_bookmark(state, tap_stream_id, 'BatchIDs', batches)
            state = singer.write_bookmark(state, tap_stream_id, 'JobHighestBookmarkSeen', current_bookmark)

        if replication_method == 'INCREMENTAL':
            replication_key = catalog_metadata.get((), {}).get('replication-key')
            replication_key_value = singer.get_bookmark(raw_state,
                                                        tap_stream_id,
                                                        replication_key)
            if version is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, 'version', version)
            if replication_key_value is not None:
                state = singer.write_bookmark(
                    state, tap_stream_id, replication_key, replication_key_value)
        elif replication_method == 'FULL_TABLE' and version is None:
            state = singer.write_bookmark(state, tap_stream_id, 'version', version)

    return state

# pylint: disable=undefined-variable
def create_property_schema(field, mdata):
    field_name = field['name']

    if field_name == "Id":
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'automatic')
    else:
        mdata = metadata.write(
            mdata, ('properties', field_name), 'inclusion', 'available')

    property_schema, mdata = quickbooks.field_to_property_schema(field, mdata)

    return (property_schema, mdata)


# pylint: disable=too-many-branches,too-many-statements
def do_discover(qb):
    """Describes a Quickbooks instance's objects and generates a JSON schema for each field."""
    objects_to_discover =  qb.describe()
    key_properties = ['Id']

    qb_custom_setting_objects = []
    object_to_tag_references = {}

    # For each SF Object describe it, loop its fields and build a schema
    entries = []

    for sobject_name in objects_to_discover:

        fields = qb.describe(sobject_name)

        replication_key = REPLICATION_KEY
        if sobject_name.endswith('Report'):
            replication_key = None


        properties = {}
        mdata = metadata.new()

        # Loop over the object's fields
        for f in fields:
            field_name = f['name']

            property_schema, mdata = create_property_schema(
                f, mdata)

            inclusion = metadata.get(
                mdata, ('properties', field_name), 'inclusion')

            if qb.select_fields_by_default:
                mdata = metadata.write(
                    mdata, ('properties', field_name), 'selected-by-default', True)

            properties[field_name] = property_schema

        if replication_key:
            mdata = metadata.write(
                mdata, ('properties', replication_key), 'inclusion', 'automatic')

        if replication_key:
            mdata = metadata.write(
                mdata, (), 'valid-replication-keys', [replication_key])
        else:
            mdata = metadata.write(
                mdata,
                (),
                'forced-replication-method',
                {
                    'replication-method': 'FULL_TABLE',
                    'reason': 'No replication keys found from the Quickbooks API'})
        if sobject_name in [
            "BalanceSheetReport",
            "MonthlyBalanceSheetReport",
            "CashFlowReport",
            "DailyCashFlowReport",
            "MonthlyCashFlowReport",
            "GeneralLedgerAccrualReport",
            "GeneralLedgerCashReport",
            "ARAgingSummaryReport",
            "ProfitAndLossDetailReport",
            "ProfitAndLossReport",
            "TransactionListReport",
        ]:
            key_properties = []
        mdata = metadata.write(mdata, (), 'table-key-properties', key_properties)

        schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': properties
        }

        entry = {
            'stream': sobject_name,
            'tap_stream_id': sobject_name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }

        entries.append(entry)

    result = {'streams': entries}
    json.dump(result, sys.stdout, indent=4)

def do_sync(qb, catalog, state, state_passed):
    starting_stream = state.get("current_stream")

    if starting_stream:
        LOGGER.info("Resuming sync from %s", starting_stream)
    else:
        LOGGER.info("Starting sync")

    for catalog_entry in catalog["streams"]:
        stream_version = get_stream_version(catalog_entry, state)
        stream = catalog_entry['stream']
        stream_alias = catalog_entry.get('stream_alias')
        stream_name = catalog_entry["tap_stream_id"]
        activate_version_message = singer.ActivateVersionMessage(
            stream=(stream_alias or stream), version=stream_version)

        catalog_metadata = metadata.to_map(catalog_entry['metadata'])
        replication_key = catalog_metadata.get((), {}).get('replication-key')

        mdata = metadata.to_map(catalog_entry['metadata'])

        if not stream_is_selected(mdata):
            LOGGER.info("%s: Skipping - not selected", stream_name)
            continue

        if starting_stream:
            if starting_stream == stream_name:
                LOGGER.info("%s: Resuming", stream_name)
                starting_stream = None
            else:
                LOGGER.info("%s: Skipping - already synced", stream_name)
                continue
        else:
            LOGGER.info("%s: Starting", stream_name)

        state["current_stream"] = stream_name
        singer.write_state(state)
        key_properties = metadata.to_map(catalog_entry['metadata']).get((), {}).get('table-key-properties')
        singer.write_schema(
            stream,
            catalog_entry['schema'],
            key_properties,
            replication_key,
            stream_alias)

        job_id = singer.get_bookmark(state, catalog_entry['tap_stream_id'], 'JobID')
        if job_id:
            with metrics.record_counter(stream) as counter:
                # Remove Job info from state once we complete this resumed query. One of a few cases could have occurred:
                # 1. The job succeeded, in which case make JobHighestBookmarkSeen the new bookmark
                # 2. The job partially completed, in which case make JobHighestBookmarkSeen the new bookmark, or
                #    existing bookmark if no bookmark exists for the Job.
                # 3. The job completely failed, in which case maintain the existing bookmark, or None if no bookmark
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('JobID', None)
                state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}).pop('BatchIDs', None)
                bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                     .pop('JobHighestBookmarkSeen', None)
                existing_bookmark = state.get('bookmarks', {}).get(catalog_entry['tap_stream_id'], {}) \
                                                              .pop(replication_key, None)
                state = singer.write_bookmark(
                    state,
                    catalog_entry['tap_stream_id'],
                    replication_key,
                    bookmark or existing_bookmark) # If job is removed, reset to existing bookmark or None
                singer.write_state(state)
        else:
            # Tables with a replication_key or an empty bookmark will emit an
            # activate_version at the beginning of their sync
            bookmark_is_empty = state.get('bookmarks', {}).get(
                catalog_entry['tap_stream_id']) is None

            if replication_key or bookmark_is_empty:
                singer.write_message(activate_version_message)
                state = singer.write_bookmark(state,
                                              catalog_entry['tap_stream_id'],
                                              'version',
                                              stream_version)
            counter = sync_stream(qb, catalog_entry, state, state_passed)
            LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter.value)

    state["current_stream"] = None
    singer.write_state(state)
    LOGGER.info("Finished sync")

def main_impl():
    args = singer_utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    LOGGER.debug(f"QB CONFIG IS {json.dumps(CONFIG)}")

    qb = None
    try:
        qb = Quickbooks(
            refresh_token=CONFIG['refresh_token'],
            qb_client_id=CONFIG['client_id'],
            qb_client_secret=CONFIG['client_secret'],
            quota_percent_total=CONFIG.get('quota_percent_total'),
            quota_percent_per_run=CONFIG.get('quota_percent_per_run'),
            is_sandbox=CONFIG.get('is_sandbox'),
            select_fields_by_default=CONFIG.get('select_fields_by_default'),
            default_start_date=CONFIG.get('start_date'),
            include_deleted = CONFIG.get('include_deleted'),
            api_type='REST',
            realm_id = CONFIG.get('realmId'),
            report_period_days = CONFIG.get('report_period_days'),
            reports_full_sync = CONFIG.get('reports_full_sync', False),
            gl_full_sync = CONFIG.get('gl_full_sync'),
            gl_weekly = CONFIG.get('gl_weekly', False),
            gl_daily = CONFIG.get('gl_daily', False),
            gl_basic_fields = CONFIG.get('gl_basic_fields', False),
            hg_sync_output = CONFIG.get('hg_sync_output')
        )
        qb.login()

        if args.discover:
            do_discover(qb)
        elif args.properties:
            catalog = args.properties
            state_passed = bool(args.state)
            state = build_state(args.state, catalog)
            do_sync(qb, catalog, state, state_passed)
    finally:
        if qb:
            if qb.rest_requests_attempted > 0:
                LOGGER.debug(
                    "This job used %s REST requests towards the Quickbooks quota.",
                    qb.rest_requests_attempted)
            if qb.login_timer:
                qb.login_timer.cancel()


def main():
    try:
        main_impl()
    except TapQuickbooksQuotaExceededException as e:
        LOGGER.critical(e)
        sys.exit(2)
    except TapQuickbooksException as e:
        LOGGER.critical(e)
        sys.exit(1)
    except Exception as e:
        LOGGER.critical(e)
        raise e

if __name__ == "__main__":
    main()