# tap-quickbooks

[Singer](https://www.singer.io/) tap that extracts data from a [Quickbooks](https://www.quickbooks.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

```bash
$ mkvirtualenv -p python3 tap-quickbooks
$ pip install tap-quickbooks
$ tap-quickbooks --config config.json --discover
$ tap-quickbooks --config config.json --properties properties.json --state state.json
```

# Quickstart

## Install the tap

```
> pip install tap-quickbooks
```

## Create a Config file

```
{
  "client_id": "secret_client_id",
  "client_secret": "secret_client_secret",
  "refresh_token": "abc123",
  "realmId": "123456789012345678901234567890"
  "start_date": "2017-11-02T00:00:00Z",
  "api_type": "REST",
  "select_fields_by_default": true
}
```

The `client_id` and `client_secret` keys are your OAuth Quickbooks App secrets. The `refresh_token` is a secret created during the OAuth flow. For more info on the Quickbooks OAuth flow, visit the [Quickbooks documentation](https://developer.quickbooks.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_web_server_oauth_flow.htm).

The `start_date` is used by the tap as a bound on SOQL queries when searching for records.  This should be an [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) formatted date-time, like "2018-01-08T00:00:00Z". For more details, see the [Singer best practices for dates](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#dates).

The `api_type` should always be set to "REST". When new fields are discovered in Quickbooks objects, the `select_fields_by_default` key describes whether or not the tap will select those fields by default.

## Run Discovery

To run discovery mode, execute the tap with the config file.

```
> tap-quickbooks --config config.json --discover > properties.json
```

## Sync Data

To sync data, select fields in the `properties.json` output and run the tap.

```
> tap-quickbooks --config config.json --properties properties.json [--state state.json]
```