# SharePoint Server Connector Reference

The [Elastic SharePoint Server connector](../connectors/sources/sharepoint_server.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Compatibility

SharePoint Server are supported by the connector.

For SharePoint Server, below mentioned versions are compatible with Elastic connector frameworks:
- SharePoint 2013
- SharePoint 2016
- SharePoint 2019

## Configuration

The following configuration fields need to be provided for setting up the connector:

#### `username`

The username of the account for SharePoint Server.

#### `password`

The password of the account to be used for the SharePoint Server.

#### `host_url`

The server host url where the SharePoint Server is hosted. Examples:

- `https://192.158.1.38:8080`

#### `site_collections`

The site collections to fetch sites from SharePoint Server(allow comma separated collections also). Examples:
- `collection1`
- `collection1, collection2`

#### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

#### `ssl_ca`

Content of SSL certificate needed for SharePoint Server.

**Note:** Keep this field empty, if `ssl_enabled` is set to `False` (Applicable on SharePoint Server only). Example certificate:


- ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    AlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHROb2RlMDExFjAUBgNV
    -----END CERTIFICATE-----
    ```

#### `retry_count`

The number of retry attempts after failed request to the SharePoint Server. Default value is `3`.

## Documents and syncs
The connector syncs the following SharePoint Server object types:
- Sites and Subsites
- Lists
- List Items and its attachment content
- Document Libraries and its attachment content(include Web Pages)

## Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## E2E Tests

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the SharePoint Server connector, run the following command:

```shell
$ make ftest NAME=sharepoint_server
```

ℹ️ Users can generate the [perf8](https://github.com/elastic/perf8) report using an argument i.e. `PERF8=True`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a SharePoint Server source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock SharePoint Server source using the docker image.

ℹ️ The connector uses the Elastic [ingest attachment processor](https://www.elastic.co/guide/en/enterprise-search/current/ingest-pipelines.html) plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.