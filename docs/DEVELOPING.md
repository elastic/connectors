# Connectors Developer's Guide

## Network Drive Connector

The [Elastic Network Drive connector](https://github.com/elastic/connectors-python/blob/main/connectors/sources/network_drive.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **technical preview**_.
Features in technical preview are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.


### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Network Drive details](#gather-network-drive-details)
2. [Configure Network Drive connector](#configure-network-drive-connector)

#### Gather Network Drive details

Collect the information that is required to connect to your network drive:

- The network drive path the connector will crawl to fetch files. This is the name of the folder shared via SMB.
- The username the connector will use to log in to network drive.
- The password the connector will use to log in to network drive.
- The server IP address where the network drive is hosted.
- The port where the network drive service is hosted.

#### Configure Network Drive connector

The following configuration fields need to be provided for setting up the connector:

##### `username`

The username of the account for network drive. Default value is `admin`.

ℹ️ The user must have atleast **read** permissions for the folder path provided.

##### `password`

The password of the account to be used for crawling the network drive.

##### `server_ip`

The server ip where network drive is hosted. Default value is `127.0.0.1`. Examples:

- `192.158.1.38`
- `127.0.0.1`

##### `server_port`

The server port where network drive service is available. Default value is `445`. Examples:

- `9454`
- `8429`

##### `drive_path`

The network drive path the connector will crawl to fetch files. Examples:

- `Users/perftest`
- `admin/bin`

ℹ️ The drive path should have forward slashes as path separators.

##### `enable_content_extraction`

Whether the connector should extract file content from network drive. Default value is `True` i.e. the connector will try to extract file contents.

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/9cf07a96288dcd542b641c51533ee2d427ef56ae/connectors/utils.py#L27).

### Connector Limitations

- Files with size greater than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to all the users having access to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently, the filtering is controlled via ingest pipelines.

### E2E Tests

The framework allows users to test the connector end to end. To perform e2e test for Network Drive connector, run the following make command:
```shell
$ make ftest NAME=network_drive
```

ℹ️ The e2e test uses default values defined in [configure Network Drive connector](#configure-network-drive-connector)


## Amazon S3 Connector

The [Elastic Amazon S3 connector](https://github.com/elastic/connectors-python/blob/main/connectors/sources/s3.py) is used to sync files and file content for supported file types from [Amazon S3](https://s3.console.aws.amazon.com/s3/home) data sources. It is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **technical preview**_.
Features in technical preview are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks.

Amazon S3 permissions required to run the connector:
  - ListAllMyBuckets
  - ListBucket
  - GetBucketLocation
  - GetObject

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Amazon S3 details](#gather-amazon-s3-details)
2. [Configure Amazon S3 connector](#configure-amazon-s3-connector)

#### Gather Amazon S3 details

Collect the information that is required to connect to your Amazon S3:

- Setup AWS configuration by installing [awscli](https://pypi.org/project/awscli/).
- Add aws_access_key, aws_secret_key and region to run the connector.


#### Configure Amazon S3 connector

The following configuration fields need to be provided for setting up the connector:

##### `buckets`

List buckets for Amazon S3. For empty list connector will fetch data for all the buckets. Examples:

  - `[testbucket, prodbucket]`
  - `[]`

##### `read_timeout`

The read_timeout for Amazon S3. Default value is `90`. Examples:

  - `60`
  - `120`

##### `connect_timeout`

The connect_timeout for crawling the Amazon S3. Default value is `90`. Examples:

  - `60`
  - `120`

##### `max_attempts`

The max_attempts for retry the Amazon S3. Default value is `5`. Examples:

  - `1`
  - `3`

##### `page_size`

The page_size for iterating bucket objects in Amazon S3. Default value is `100`. Examples:

  - `50`
  - `150`

##### `enable_content_extraction`

Whether the connector should extract file content from Amazon S3. Default value is `True` i.e. the connector will try to extract file contents.

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/9cf07a96288dcd542b641c51533ee2d427ef56ae/connectors/utils.py#L27).

### Connector Limitations

- Files with size greater than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to all the users having access to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently, the filtering is controlled via ingest pipelines.
- The user needs to set a profile with the AWS configure command.
- Currently the connector does not support S3 compatible vendors.

### E2E Tests

The framework allows users to test the connector end to end. To perform e2e test for Amazon S3 connector, run the following make command:
```shell
$ make ftest NAME=s3
```

ℹ️ The e2e test uses default values defined in [configure Amazon S3 connector](#configure-amazon-s3-connector)

## Installation

Provides a CLI to ingest documents into Elasticsearch, following the [connector protocol](https://github.com/elastic/connectors-ruby/blob/main/docs/CONNECTOR_PROTOCOL.md).

To install the CLI, run:
```shell
$ make install
```

The `elastic-ingest` CLI will be installed on your system:

```shell
$ bin/elastic-ingest --help
usage: elastic-ingest [-h] [--action {poll,list}] [-c CONFIG_FILE] [--debug]

optional arguments:
-h, --help            show this help message and exit
--action {poll,list}  What elastic-ingest should do
-c CONFIG_FILE, --config-file CONFIG_FILE
                        Configuration file
--debug               Run the event loop in debug mode
```

# Architecture

The CLI runs the [ConnectorService](../connectors/runner.py) which is an asynchronous event loop. It calls Elasticsearch on a regular basis to see if some syncs need to happen.

That information is provided by Kibana and follows the [connector protocol](https://github.com/elastic/connectors-ruby/blob/main/docs/CONNECTOR_PROTOCOL.md). That protocol defines a few structures in a couple of dedicated Elasticsearch indices, that are used by Kibana to drive sync jobs, and by the connectors to report on that work.

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
