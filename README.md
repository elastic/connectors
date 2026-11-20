[![Build status](https://badge.buildkite.com/a1319036cb613e63515320f44b187cd233771715c811d3dc7a.svg?branch=main)](https://buildkite.com/elastic/connectors)
# Elastic connectors (aka crawlers)

<img width="250" alt="search-icon" src="https://github.com/elastic/connectors/assets/32779855/2f594d89-7369-4c49-994a-1d67eefce436">

Use connectors to sync data from popular data sources to Elasticsearch.

This repository contains the source code for all Elastic connectors, also known as crawlers.
Kibana 9.7 and later shows the UI page for managing connectors as Content crawlers.

Self-managed connectors run on your own infrastructure. For an overview of the deployment steps, refer to [Self-managed connectors](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-build-connector.html) in the official Elastic documentation.

To get started quickly using Docker Compose, check out this [README file](./app/connectors_service/scripts/stack/README.md).

### Documentation

The main documentation lives in the [Elastic docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors.html). Check here for individual data source references and deployment information.

Since 8.12.0, you can manage connectors and sync jobs programmatically using APIs.
Refer to the [Connector API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/v8/group/endpoint-connector).

Everything else concerning _developing_ connectors is located in this repo.

### Command-line interface

Learn about our CLI tool in [`docs/CLI.md`](./docs/CLI.md).

### Project structure

There are two main directories; the service code lives in the `app/` directory and the SDK and framework in `libs/`.

The Connectors SDK is a dependency of the connectors application. You can use the Connectors SDK in and of itself to create your own data source implementations without needing to ingest data into Elasticsearch.

#### Service code `app/`

This service defines tasks such as running connectors and managing scheduling, syncs, and cleanup.
This is shared code that is not used by individual connectors, but helps to coordinate and run a deployed instance/process.

#### SDK and framework `libs/`

The Connector SDK provides a framework that enables developers to build self-managed connectors.
The framework implements common functionalities out of the box, so developers can focus on the logic specific to integrating their chosen data source.

The framework ensures compatibility, makes it easier for our team to review PRs, and helps out in the development process. When you build using our framework, we provide a pathway for the connector to be officially supported by Elastic.

### Running a self-managed stack

This repo provides a [set of scripts](./app/connectors_service/scripts/stack) to allow a user to set up a full Elasticsearch, Kibana, and Connectors service stack using Docker.
This is useful to get up and running with minimal effort, and provides a guided set of prompts for setup and configuration.
For more information, instructions, and options, see the [README file](./app/connectors_service/scripts/stack/README.md) in the stack folder.

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
> Version compatibility is not checked when Elasticsearch is **serverless** (the connector service skips the check on startup).

The connector service compares its version to the configured Elasticsearch server on startup.
If the versions are incompatible, the service exits and logs the mismatch.
If the versions differ but are compatible, the service logs a warning and continues.

We strongly recommend running the connector service on the same stack version as Elasticsearch.
Use this table to decide whether a deliberate version skew is supported.

Do **not downgrade** the connector service to an older release.
If downgraded, connector configuration stored in Elasticsearch may no longer match what the older service expects and affected connectors will need to be **recreated**.

| Situation | Example connector version | Example ES version | Outcome |
|-----------|---------------------------|--------------------|---------|
| Versions match | 8.15.1.0 | 8.15.1 | OK |
| Connector has a build suffix | 8.15.1.3 | 8.15.1 | OK |
| ES patch is newer | 8.15.__0__.0 | 8.15.__1__ | Warning |
| ES minor is newer | 8.__14__.2.0 | 8.__15__.0 | Warning |
| ES major is newer | __8__.15.1.0 | __9__.0.0 | Fatal error |
| ES patch is older | 8.15.__1__.0 | 8.15.__0__ | Warning |
| ES minor is older | 8.__15__.1.0 | 8.__14__.2 | Fatal error |
| ES major is older | __9__.0.0.0 | __8__.15.1 | Fatal error |
