# Confluence Connector Reference

The [Elastic Confluence connector](../connectors/sources/confluence.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Compatibility

Confluence Cloud or Confluence Server versions 7 or later are compatible with Elastic connector frameworks. Confluence Data Center editions are not currently supported.

## Configuration

The following configuration fields need to be provided for setting up the connector:

#### `data_source`

Dropdown to determine the Confluence platform type: `Confluence Cloud` or `Confluence Server`. Default value is `Confluence Server`.

#### `username`

The username of the account for Confluence server.

#### `password`

The password of the account to be used for the Confluence server.

#### `account_email`

The account email for the Confluence cloud.

#### `api_token`

The API Token to authenticate with Confluence cloud.

#### `confluence_url`

The domain where the Confluence is hosted. Examples:

- `https://192.158.1.38:8080/`
- `https://test_user.atlassian.net/`

#### `spaces`

Comma-separated list of [Space Keys](https://confluence.atlassian.com/doc/space-keys-829076188.html) to fetch data from Confluence server or cloud. If the value is `*`, the connector will fetch data from all spaces present in the configured `spaces` . Default value is `*`. Examples:

- `EC, TP`
- `*`

#### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

#### `ssl_ca`

Content of SSL certificate. Note: In case of ssl_enabled is `False`, keep `ssl_ca` field empty. Example certificate:

- ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

#### `retry_count`

The number of retry attempts after failed request to Confluence. Default value is `3`.

#### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `50`.

## Documents and syncs

The connector syncs the following Confluence object types:
- **Pages**
- **Spaces**
- **Blog Posts**
- **Attachments**

## Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the Confluence connector, run the following command:

```shell
$ make ftest NAME=confluence
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Confluence source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Confluence source using the docker image.

ℹ️ The e2e test uses default values defined in [Configure Confluence connector](#configure-confluence-connector)

## Known issues

There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).