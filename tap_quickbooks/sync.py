import os
import time
import singer
import singer.utils as singer_utils
from singer import Transformer, metadata, metrics
from requests.exceptions import RequestException
from jsonpath_ng import jsonpath, parse
import math
import json
import requests

LOGGER = singer.get_logger()


# __null__ = ""
# pylint: disable=unused-argument
def transform_data_hook(data, typ, schema):
    result = data
    if isinstance(data, dict):
        result = {}
        schema_properties = schema.get('properties', {})
        for property in schema_properties:
            data_property = data.get(property, None)
            result[property] = data_property

        if not typ == 'object':
            result = json.dumps(data)

    # Quickbooks can return the value '0.0' for integer typed fields. This
    # causes a schema violation. Convert it to '0' if schema['type'] has
    # integer.
    if data == '0.0' and 'integer' in schema.get('type', []):
        result = '0'

    # Quickbooks Bulk API returns CSV's with empty strings for text fields.
    # When the text field is nillable and the data value is an empty string,
    # change the data so that it is None.
    if data == "" and "null" in schema['type']:
        result = None

    return result


def get_stream_version(catalog_entry, state):
    tap_stream_id = catalog_entry['tap_stream_id']
    catalog_metadata = metadata.to_map(catalog_entry['metadata'])
    replication_key = catalog_metadata.get((), {}).get('replication-key')

    if singer.get_bookmark(state, tap_stream_id, 'version') is None:
        stream_version = int(time.time() * 1000)
    else:
        stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if replication_key:
        return stream_version
    return int(time.time() * 1000)


def sync_stream(qb, catalog_entry, state, state_passed):
    stream = catalog_entry['stream']

    with metrics.record_counter(stream) as counter:
        try:
            sync_records(qb, catalog_entry, state, counter, state_passed)
            singer.write_state(state)
        except RequestException as ex:
            raise Exception("Error syncing {}: {} Response: {}".format(
                stream, ex, ex.response.text))
        except Exception as ex:
            raise Exception("Error syncing {}: {}".format(
                stream, ex)) from ex

        return counter


def sync_records(qb, catalog_entry, state, counter, state_passed):
    chunked_bookmark = singer_utils.strptime_with_tz(qb.get_start_date(state, catalog_entry))
    stream = catalog_entry['stream']
    schema = catalog_entry['schema']
    stream_alias = catalog_entry.get('stream_alias')
    catalog_metadata = metadata.to_map(catalog_entry['metadata'])
    replication_key = catalog_metadata.get((), {}).get('replication-key')
    stream_version = get_stream_version(catalog_entry, state)
    activate_version_message = singer.ActivateVersionMessage(stream=(stream_alias or stream),
                                                             version=stream_version)

    start_time = singer_utils.now()

    LOGGER.info('Syncing Quickbooks data for stream %s', stream)

    previous_max_replication_key = None;

    query_func = qb.query
    if stream.endswith("Report"):
        query_func = qb.query_report

    for rec in query_func(catalog_entry, state, state_passed):
        #Check if it is Attachable stream with a downloadable file
        if stream == 'Attachable' and "TempDownloadUri" in rec:
            file_name = rec["FileName"]
            attachable_ref = rec.get("AttachableRef",[])
            if len(attachable_ref)>0 and "EntityRef" in attachable_ref[0]:
                attachable_ref = attachable_ref[0]['EntityRef']
                if attachable_ref:
                    file_name = f"{attachable_ref['type']}-{attachable_ref['value']}-{file_name}"
            #Save the newly formatted file name        
            rec['FileName'] = file_name      
            download_file(
                rec["TempDownloadUri"], os.path.join(qb.hg_sync_output or "", file_name)
            )
        counter.increment()
        with Transformer(pre_hook=transform_data_hook) as transformer:
            rec = transformer.transform(rec, schema)

        singer.write_message(
            singer.RecordMessage(
                stream=(
                        stream_alias or stream),
                record=rec,
                version=stream_version,
                time_extracted=start_time))

        if replication_key:
            jsonpath_expression = parse(f"$.{replication_key}")
            _rec = {'MetaData': json.loads(rec.get('MetaData', "{}"))}
            match = jsonpath_expression.find(_rec)
            original_replication_key_value = ""
            if replication_key and len(match) > 0:
                original_replication_key_value = match[0].value
                replication_key_value = singer_utils.strptime_with_tz(original_replication_key_value)
            else:
                replication_key_value = None

            # Before writing a bookmark, make sure Quickbooks has not given us a
            # record with one outside our range
            if previous_max_replication_key is None or (
                    replication_key_value and replication_key_value <= start_time and replication_key_value > previous_max_replication_key
            ):
                state = singer.write_bookmark(
                    state,
                    catalog_entry['tap_stream_id'],
                    replication_key,
                    original_replication_key_value)
                previous_max_replication_key = replication_key_value

            # Tables with no replication_key will send an
            # activate_version message for the next sync

    if not replication_key:
        singer.write_message(activate_version_message)
        state = singer.write_bookmark(
            state, catalog_entry['tap_stream_id'], 'version', None)

def download_file(url, local_filename):
    # Send an HTTP GET request to the URL
    response = requests.get(url, stream=True)
    LOGGER.info(f"Downloading file: {local_filename}")
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a local file with write-binary mode to save the downloaded content
        with open(local_filename, 'wb') as f:
            # Iterate over the content of the response in chunks and write to the file
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        LOGGER.info(f"File downloaded successfully: {local_filename}")
    else:
        LOGGER.info(f"Failed to download file. HTTP status code: {response.status_code}")
        
