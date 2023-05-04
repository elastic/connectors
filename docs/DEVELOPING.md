# Connectors Developer's Guide

ℹ️ Find documentation for the following connector clients in the Elastic Enterprise Search docs:

- [Azure Blob Storage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-azure-blob.html)
- [Google Cloud Storage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-google-cloud.html)
- [Microsoft SQL](https://www.elastic.co/guide/en/enterprise-search/master/connectors-ms-sql.html)
- [MongoDB](https://www.elastic.co/guide/en/enterprise-search/master/connectors-mongodb.html)
- [MySQL](https://www.elastic.co/guide/en/enterprise-search/master/connectors-mysql.html)
- [Network drive](https://www.elastic.co/guide/en/enterprise-search/master/connectors-network-drive.html)
- [Oracle](https://www.elastic.co/guide/en/enterprise-search/master/connectors-oracle.html)
- [PostgreSQL](https://www.elastic.co/guide/en/enterprise-search/master/connectors-postgresql.html)
- [S3](https://www.elastic.co/guide/en/enterprise-search/master/connectors-s3.html)

## Confluence Connector

The [Elastic Confluence connector](../connectors/sources/confluence.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

### Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

### Compatibility

Confluence Cloud or Confluence Server versions 7 or later are compatible with Elastic connector frameworks. Confluence Data Center editions are not currently supported.

### Configuration

The following configuration fields need to be provided for setting up the connector:

##### `data_source`

Dropdown to determine the Confluence platform type: `Confluence Cloud` or `Confluence Server`. Default value is `Confluence Server`.

##### `username`

The username of the account for Confluence server.

##### `password`

The password of the account to be used for the Confluence server.

##### `account_email`

The account email for the Confluence cloud.

##### `api_token`

The API Token to authenticate with Confluence cloud.

##### `confluence_url`

The domain where the Confluence is hosted. Examples:

  - `https://192.158.1.38:8080/`
  - `https://test_user.atlassian.net/`

##### `spaces`

Comma-separated list of [Space Keys](https://confluence.atlassian.com/doc/space-keys-829076188.html) to fetch data from Confluence server or cloud. If the value is `*`, the connector will fetch data from all spaces present in the configured `spaces` . Default value is `*`. Examples:

  - `EC, TP`
  - `*`

##### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

##### `ssl_ca`

Content of SSL certificate. Note: In case of ssl_enabled is `False`, keep `ssl_ca` field empty. Example certificate:

  - ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

##### `retry_count`

The number of retry attempts after failed request to Confluence. Default value is `3`.

##### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `50`.


#### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.

### Documents and syncs

The connector syncs the following Confluence object types: 
- **Pages**
- **Spaces**
- **Blog Posts**
- **Attachments**

### Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

### Connector Client operations

#### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the Confluence connector, run the following command:

```shell
$ make ftest NAME=confluence
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Confluence source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Confluence source using the docker image.

ℹ️ The e2e test uses default values defined in [Configure Confluence connector](#configure-confluence-connector)

### Known issues

There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

### Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

### Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

## Jira Connector

The [Elastic Jira connector](../connectors/sources/jira.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**.  This connector is in **beta** and is subject to change. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

### Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

### Compatibility

Jira Cloud or Jira Server versions 7 or later are compatible with Elastic connector frameworks. Jira Data Center editions are not currently supported.

### Configuration

#### Configure Jira connector

The following configuration fields need to be provided for setting up the connector:

##### `data_source`

Dropdown to determine Jira platform type: `Jira Cloud` or `Jira Server`. Default value is `Jira Cloud`.

##### `username`

The username of the account for Jira server.

##### `password`

The password of the account to be used for Jira server.

##### `account_email`

The account email for Jira cloud.

##### `api_token`

The API Token to authenticate with Jira cloud.

##### `jira_url`

The domain where Jira is hosted. Examples:

  - `https://192.158.1.38:8080/`
  - `https://test_user.atlassian.net/`

##### `projects`

Comma-separated list of [Project Keys](https://support.atlassian.com/jira-software-cloud/docs/what-is-an-issue/#Workingwithissues-Projectkeys) to fetch data from Jira server or cloud. If the value is `*` the connector will fetch data from all projects present in the configured `projects` . Default value is `*`. Examples:

  - `EC, TP`
  - `*`

##### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

##### `ssl_ca`

Content of SSL certificate. Note: In case of ssl_enabled is `False`, keep `ssl_ca` field empty. Example certificate:

  - ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

##### `retry_count`

The number of retry attempts after failed request to Jira. Default value is `3`.

##### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `100`.

#### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.

### Documents and Sync

The connector syncs the following objects and entities:
- **Projects**
  - Includes metadata such as description, project key, project type, lead name, etc.
- **Issues**
  - All types of issues including Task, Bug, Sub-task, Enhancement, Story, etc.
  - Includes metadata such as issue type, parent issue details, fix versions, affected versions, resolution, attachments, comments, sub-task details, priority, custom fields, etc.
- **Attachments**

**Note:** Archived projects and issues are not indexed.

### Sync rules

- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

### Connector Client operations

#### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Jira connector, run the following command:

```shell
$ make ftest NAME=jira
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Jira source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Jira source using the docker image.

### Known issues

There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

### Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

### Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

## General Configuration

The details of Elastic instance and other relevant fields such as `service` and `source` needs to be provided in the [config.yml](https://github.com/elastic/connectors-python/blob/8.6/config.yml) file. For more details check out the following [documentation](https://github.com/elastic/connectors-python/blob/8.6/docs/CONFIG.md).

## Installation

Provides a CLI to ingest documents into Elasticsearch, following the [connector protocol](https://github.com/elastic/connectors-python/blob/main/docs/CONNECTOR_PROTOCOL.md).

To install the CLI, run:
```shell
$ make install
```

The `elastic-ingest` CLI will be installed on your system:

```shell
$ bin/elastic-ingest --help
usage: elastic-ingest [-h] [--action {poll,list}] [-c CONFIG_FILE] [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL} | --debug] [--filebeat] [--version] [--uvloop]

options:
  -h, --help            show this help message and exit
  --action {poll,list}  What elastic-ingest should do
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        Configuration file
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set log level for the service.
  --debug               Run the event loop in debug mode (alias for --log-level DEBUG)
  --filebeat            Output in filebeat format.
  --version             Display the version and exit.
  --uvloop              Use uvloop if possible
```

Users can execute `make run` command to run the elastic-ingest process in `debug` mode. For more details check out the following [documentation](./CONFIG.md)

# Architecture

The CLI runs the [ConnectorService](../connectors/runner.py) which is an asynchronous event loop. It calls Elasticsearch on a regular basis to see if some syncs need to happen.

That information is provided by Kibana and follows the [connector protocol](https://github.com/elastic/connectors-python/blob/main/docs/CONNECTOR_PROTOCOL.md). That protocol defines a few structures in a couple of dedicated Elasticsearch indices, that are used by Kibana to drive sync jobs, and by the connectors to report on that work.

When a user asks for a sync of a specific source, the service instantiates a class that it uses to reach out the source and collect data.

A source class can be any Python class, and is declared into the [configuration](../config.yml) file (See [Configuration](./CONFIG.md) for detailed explanation). For example:

```yaml
sources:
  mongodb: connectors.sources.mongo:MongoDataSource
  s3: connectors.sources.s3:S3DataSource
```

The source class is declared with its [Fully Qualified Name(FQN)](https://en.wikipedia.org/wiki/Fully_qualified_name) so the framework knows where the class is located, so it can import it and instantiate it.

Source classes can be located in this project or any other Python project, as long as it can be imported.

For example, if the project `mycompany-foo` implements the source `GoogleDriveDataSource` in the package `gdrive`, we should be able to run:

```shell
$ pip install mycompany-foo
```

And then add in the Yaml file:

```yaml
sources:
  gdrive: gdrive:GoogleDriveDataSource
```

And that source will be available in Kibana.

# Sync strategy

In Workplace Search we have the four following syncs:

- **Full sync** (runs every 72 hours by default): This synchronization job synchronizes all the data from the content source ensuring full data parity.
- **Incremental sync** (runs every 2 hours by default): This synchronization job synchronizes updates to the data at the content source ensuring high data freshness.
- **Deletion sync** (runs every 6 hours by default): This synchronization job synchronizes document deletions from the content source ensuring regular removal of stale data.
- **Permissions sync** (runs every 5 minutes by default, when Document Level Permissions are enabled): This synchronization job synchronizes document permissions from the content sources ensuring secure access to documents on Workplace Search.

In Elastic Python connectors we are implementing for now just **Full sync**, which ensures full data parity (including deletion).

This sync strategy is good enough for some sources like MongoDB where 100,000 documents can be fully synced in less than 30 seconds.

We will introduce more sophisticated syncs as we add new sources, in order to achieve the same level of freshness we have in Workplace Search.

The **Permissions sync** will be included later as well once we have designed how Document-Level Permission works in the new architecture.

# How a sync works

Syncing a backend consists of reconciliating an Elasticsearch index with an external data source. It's a read-only mirror of the data located in the 3rd party data source.

To sync both sides, the CLI uses these steps:

- asks the source if something has changed, if not, bail out.
- collects the list of documents IDs and timestamps in Elasticsearch
- iterate on documents provided by the data source class
- for each document
  - if there is a timestamp, and it matches the one in Elasticsearch, ignores it
  - if not, adds it as an `upsert` operation into a `bulk` call to Elasticsearch
- for each id from Elasticsearch that is not present in the documents sent by the data source class, adds it as a `delete` operation into the `bulk` call
- `bulk` calls are emitted every 500 operations (this is configurable for slow networks).

To implement a new source, check [CONTRIBUTE.rst](./CONTRIBUTING.md)

## Runtime dependencies

- MacOS or Linux server. The connector has been tested with CentOS 7, MacOS Monterey v12.0.1.
- Python version 3.10 or later.
- To fix SSL certificate verification failed error, users have to run this to connect with Amazon S3:
    ```shell
    $ System/Volumes/Data/Applications/Python\ 3.10/Install\ Certificates.command
    ```
