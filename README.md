# tap-quickbooks
**tap-quickbooks** is a Singer Tap for syncing data from Quickbooks Online. 
**tap-quickbooks** can be run on [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

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
  "realmId": "123456789012345678901234567890",
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

## Downloading Attachments

QuickBooks Online exposes file attachments through the `Attachable` entity, which
links a file to one or more transactions (Invoice, Bill, PurchaseOrder, etc.) via
`AttachableRef[]`. Each `Attachable` record includes a pre-signed `TempDownloadUri`.

When the `Attachable` stream is selected, the tap can download the underlying file
bytes for each record. Downloading is opt-in via the `download_attachments` config
flag.

### Config options

```json
{
  "download_attachments": true
}
```

- `download_attachments` (boolean, default `false`) - master on/off switch for
  fetching attachment file bytes. When `false`, `Attachable` metadata records are
  still emitted but no files are downloaded.

### Output layout

Downloaded files are grouped by the linked entity under the sync-output root:

```
<sync-output>/{entity_type}_attachments/{entity_id}/{file_name}
```

For example:

```
<sync-output>/bill_attachments/130/receipt.pdf
<sync-output>/invoice_attachments/42/logo.png
```

`entity_type` is the lowercased QuickBooks entity (e.g. `bill`, `invoice`) taken
from the attachment's `AttachableRef`. Attachments not linked to any entity are
written to a flat `attachments/{file_name}` folder.

The sync-output root is resolved as follows: when the `JOB_ID` environment variable
is set (hotglue), the base is `/home/hotglue/{JOB_ID}/sync-output`; otherwise it is
`./sync-output`. A `hg_sync_output` config value, if provided, overrides this base.
