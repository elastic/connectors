[![Build status](https://badge.buildkite.com/a1319036cb613e63515320f44b187cd233771715c811d3dc7a.svg?branch=main)](https://buildkite.com/elastic/connectors)
# Elastic connectors

<img width="250" alt="search-icon" src="https://github.com/elastic/connectors/assets/32779855/2f594d89-7369-4c49-994a-1d67eefce436">

## Connectors

This repository contains the source code for all Elastic connectors, developed by the Search team at Elastic.
Use connectors to sync data from popular data sources to Elasticsearch.

These connectors are available to be self-managed on your own infrastructure.

ℹ️ For an overview of the steps involved in deploying connectors refer to [**Self-managed connectors**](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-build-connector.html) in the official Elastic documentation.

To get started quickly with self-managed connectors using Docker Compose, check out this [README file](./scripts/stack/README.md).

### Connector documentation

The main documentation for using connectors lives in the [Elastic docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors.html).
You'll find the individual references for each connector there.

For everything to do with _developing_ connectors, you'll find that here in this repo.

#### API documentation

Since 8.12.0, you can manage connectors and sync jobs programmatically using APIs.
Refer to the [Connector API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/v8/group/endpoint-connector).

#### Command-line interface

Learn about our CLI tool in [`docs/CLI.md`](./docs/CLI.md).

## Connector service code

In addition to the source code for individual connectors, this repo also contains the connector service code, which is used for tasks like running connectors, and managing scheduling, syncs, and cleanup.
This is shared code that is not used by individual connectors, but helps to coordinate and run a deployed instance/process.

## Connector framework

This repo is also the home of the Elastic connector framework. This framework enables developers to build Elastic-supported self-managed connectors.
The framework implements common functionalities out of the box, so developers can focus on the logic specific to integrating their chosen data source.

The framework ensures compatibility, makes it easier for our team to review PRs, and help out in the development process. When you build using our framework, we provide a pathway for the connector to be officially supported by Elastic.

## Running a self-managed stack

This repo provides a [set of scripts](./scripts/stack) to allow a user to set up a full Elasticsearch, Kibana, and Connectors service stack using Docker.
This is useful to get up and running with the Connectors framework with minimal effort, and provides a guided set of prompts for setup and configuration.
For more information, instructions, and options, see the [README file](./scripts/stack/README.md) in the stack folder.

### Framework use cases

The framework serves two distinct, but related use cases:

- Customizing an existing Elastic connector client
- Building a new self-managed connector

### Guides for using the framework

- [Code of Conduct](https://www.elastic.co/community/codeofconduct)
- [Getting Support](docs/SUPPORT.md)
- [Releasing](docs/RELEASING.md)
- [Developer guide](docs/DEVELOPING.md)
- [Connectors Reference](docs/REFERENCE.md)
- [Security Policy](docs/SECURITY.md)
- [Elastic-internal guide](docs/INTERNAL.md)
- [Connector Protocol](docs/CONNECTOR_PROTOCOL.md)
- [Configuration](docs/CONFIG.md)
- [Command line interface](docs/CLI.md)
- [Contribution guide](docs/CONTRIBUTING.md)
- [Upgrading](docs/UPGRADING.md)

### Version compatibility with Elasticsearch

> [!NOTE]
> Version compatibility will not be checked if Elasticsearch is serverless.

The Connector will perform a version compatibility check with the configured Elasticsearch server on startup.
If the versions are incompatible, the Connector will terminate and output the incompatible versions in the shell.
If the versions are different but otherwise compatible, the Connector will output a warning in the shell but will continue operating.

We recommend running on the same version as Elasticsearch.
However, if you want to hold back upgrading one or the other for any reason, use this table to determine if your versions will be compatible.

| Situation                       | Example Connector Framework version | Example ES version | Outcome |
|---------------------------------|-------------------------------------|--------------------| ------- |
| Versions are the same.          | 8.15.1.0                            | 8.15.1             | 💚 OK      |
| Connectors has a build version. | 8.15.1.3                            | 8.15.1             | 💚 OK      |
| ES patch number is newer.       | 8.15.__0__.0                        | 8.15.__1__         | ⚠️ Logged warning      |
| ES minor number is newer.       | 8.__14__.2.0                        | 8.__15__.0         | ⚠️ Logged warning      |
| ES major number is newer.       | __8__.15.1.0                        | __9__.0.0          | 🚫 Fatal error      |
| ES patch number is older.       | 8.15.__1__.0                        | 8.15.__0__         | ⚠️ Logged warning      |
| ES minor number is older.       | 8.__15__.1.0                        | 8.__14__.2         | 🚫 Fatal error      |
| ES major number is older.       | __9__.0.0.0                         | __8__.15.1         | 🚫 Fatal error      |
