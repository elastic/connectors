# Opentext Documentum Connector

The Elastic Opentext Documentum connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main). View the [source code for this connector](https://github.com/elastic/connectors/blob/main/connectors/sources/opentext_documentum.py).

## Availability and prerequisites

This connector is available as a self-managed connector client.
To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is an **tech preview connector** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties.

## Usage

To use this connector as a **connector client**, select the **Opentext Documentum** tile when creating a new connector under **Search -> Connectors**.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to Opentext Documentum

Username and password based authentication is used to connect with Opentext Documentum.

Once completed, use the following parameters to configure the connector.

## Configuration

### Configure Opentext Documentum connector

Note the following configuration fields:

#### `Opentext Documentum host url`  (required)

The domain where Opentext Documentum is hosted. Example:

- `https://192.158.1.38:2099/`

#### `Username`  (required)

The username of the account to connect with Opentext Documentum.

#### `Password`  (required)

The password of the account to connect with Opentext Documentum.

#### `Repositories`

Comma-separated list of repositories to fetch data from Opentext Documentum. If the value is `*` the connector will fetch data from all repositories present in the configured user’s account.

Default value is `*`.

Examples:

- `elastic`, `kibana`
- `*`

#### `Enable SSL`

Enable SSL for the Opentext Documentum instance.

#### `SSL Certificate`

SSL certificate for the Opentext Documentum instance. Example:

```
-----BEGIN CERTIFICATE-----
MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
...
7RhLQyWn2u00L7/9Omw=
-----END CERTIFICATE-----
```

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Repositories**
- **Cabinets**
- **Files & Folders**

*NOTE*:
- Files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to the destination Elasticsearch index.

### Sync types
[Full syncs](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sync-types.html#connectors-sync-types-full) are supported by default for all connectors.

This connector currently does not support [incremental syncs](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sync-types.html#connectors-sync-types-incremental).

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Opentext Documentum connector, run the following command:

```shell
$ make ftest NAME=opentext_documentum
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Opentext Documentum source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Opentext Documentum source using the docker image.

## Known issues

- There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
