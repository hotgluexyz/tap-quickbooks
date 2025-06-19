import os
from typing import ClassVar, Dict, List, Optional

import attr
import backoff
import requests
import singer
import time

from tap_quickbooks.util import save_api_usage

LOGGER = singer.get_logger()


def is_fatal_code(e: requests.exceptions.RequestException) -> bool:
    '''Helper function to determine if a Requests reponse status code
    is a "fatal" status code. If it is, the backoff decorator will giveup
    instead of attemtping to backoff.'''
    return 400 <= e.response.status_code < 500 and e.response.status_code not in [429, 400]

class RetriableException(Exception):
    pass

@attr.s
class QuickbooksStream:

    api_minor_version: ClassVar[int] = 40

    def _get_abs_path(self, path: str) -> str:
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    @backoff.on_exception(backoff.fibo,
                          requests.exceptions.HTTPError,
                          max_tries=5,
                          giveup=is_fatal_code)
    @backoff.on_exception(backoff.fibo,
                          (requests.exceptions.ConnectionError,
                           requests.exceptions.Timeout,
                           RetriableException
                           ),
                          max_tries=5)
    def _get(self, report_entity: str, params: Optional[Dict] = None) -> Dict:
        '''Constructs a standard way of making
        a GET request to the Quickbooks REST API.
        '''
        url = f"{self.qb.instance_url}/reports/{report_entity}"
        headers = self.qb._get_standard_headers()

        if params:
            params.update({"minorversion": self.api_minor_version})

        response = requests.get(url, headers=headers, params=params)

        try:
            save_api_usage("GET", url, params, None, response, self.stream)
        except Exception as e:
            LOGGER.error("Error saving API usage: %s", str(e))

        if response.status_code == 429:
            # quickbooks: HTTP Status Code 429 happens when throttling occurs. 
            # Wait 60 seconds before retrying the request.
            time.sleep(60)
        response.raise_for_status()

        # handle random empty responses or not valid json responses
        try:
            res_json = response.json()
        except:
            raise RetriableException(f"Invalid json response: {response.text} ")
        
        if res_json == None:
            raise RetriableException(f"Empty response returned {response.text} ")
        
        return res_json

    def _convert_string_value_to_float(self, value: str) -> float:
        '''Safely converts string values to floats.'''
        if value == "":
            float_value = float(0.0)
        else:
            float_value = float(value)
        return float_value

    def _get_row_data(self, resp, column_enum, input):
        for row in resp.get("Rows").get("Row"):
            if row.get("type") == "Section" and row.get("Rows") is not None:
                data_dict = {}
                key = row.get("Header").get("ColData")[0].get("value").title().replace(" ", "")
                data = {
                    key: {
                        "Lines": [],
                        "Total": self._convert_string_value_to_float(row.get("Summary").get("ColData")[column_enum].get("value"))
                    }
                }
                data_dict.update(data)
                input.append(data_dict)
                self._get_row_data(resp=row, input=data_dict[key]["Lines"], column_enum=column_enum)
            elif row.get("Summary") is not None:
                data_dict = {}
                key = row.get("Summary").get("ColData")[0].get("value").title().replace(" ", "")
                data = {
                    key: {
                        "Lines": [],
                        "Total": self._convert_string_value_to_float(row.get("Summary").get("ColData")[column_enum].get("value"))
                    }
                }
                data_dict.update(data)
                input.append(data_dict)
            else:
                data_dict = {}
                key = row.get("ColData")[0].get("value").title().replace(" ", "")
                data = {
                    key: {
                        "Lines": [],
                        "Total": self._convert_string_value_to_float(row.get("ColData")[column_enum].get("value"))
                    }
                }
                data_dict.update(data)
                input.append(data_dict)

        return input

    def _transform_columns_into_rows(self, resp):
        records = []
        for column in resp.get("Columns").get("Column"):
            if column.get("ColType") == "Money":
                record = {}
                for meta_column in column.get("MetaData"):
                    record[meta_column.get("Name")] = meta_column.get("Value")
                records.append(record)

        return records

    def write_schema_message(self):
        '''Writes a Singer schema message.'''
        return singer.write_schema(stream_name=self.stream, schema=self.schema, key_properties=self.key_properties)