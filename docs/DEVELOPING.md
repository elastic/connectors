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
