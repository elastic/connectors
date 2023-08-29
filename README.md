# Elastic Python connectors

<img width="250" alt="search-icon" src="https://github.com/elastic/connectors-python/assets/32779855/2f594d89-7369-4c49-994a-1d67eefce436">

## Connectors

This repository contains the source code for all Elastic connectors, developed by the Search team at Elastic.
Use connectors to sync data from popular data sources to Elasticsearch.

These connectors are available as:
- [**Connector clients**](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html) to be self-managed on your own infrastructure
- [**Native connectors**](https://www.elastic.co/guide/en/enterprise-search/current/native-connectors.html) using our fully managed service on Elastic Cloud

ℹ️ For an overview of the steps involved in deploying connector clients refer to [**Connector clients**](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).
You can get started quickly with Docker, using [these instructions](docs/DOCKER.md).
We also have an end-to-end tutorial using the [PostgreSQL connector client](https://www.elastic.co/guide/en/enterprise-search/current/postgresql-connector-client-tutorial.html).

### Connector documentation

The main documentation for _using_ connectors lives in the Search solution's docs.
Here are the main pages:

- [Connectors overview](https://www.elastic.co/guide/en/enterprise-search/current/connectors.html)
- [Connector clients](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html)
- [Native connectors](https://www.elastic.co/guide/en/enterprise-search/current/native-connectors.html)

You'll also find the individual references for each connector there.
For everything to do with _developing_ connectors, you'll find that here in this repo.

## Python connector framework

This repo also contains the Elastic Python connector framework. This framework enables developers to build Elastic-supported connector clients.
The framework implements common functionalities out of the box, so developers can focus on the logic specific to integrating their chosen data source.

The framework ensures compatibility, makes it easier for our team to review PRs, and help out in the development process. When you build using our framework, we provide a pathway for the connector to be officially supported by Elastic.

### Framework use cases

The framework serves two distinct, but related use cases:

- Customizing an existing Elastic connector client
- Building a new connector client

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
- [Contribution guide](docs/CONTRIBUTING.md)
- [Upgrading](docs/UPGRADING.md)
