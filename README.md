# Elastic Python connectors

<img width="468" alt="search-icon" src="https://github.com/elastic/connectors-python/assets/32779855/2f594d89-7369-4c49-994a-1d67eefce436">

## Connectors

This repository contains the source code for all Elastic connectors, developed by the Search team at Elastic.
Use connectors to sync data from popular data sources to Elasticsearch.

These connectors are available as:
- [**Connector clients**](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html) to be self-managed on your own infrastructure
- [**Native connectors**](https://www.elastic.co/guide/en/enterprise-search/current/native-connectors.html) using our fully managed service on Elastic Cloud

ℹ️ For an overview of the steps involved in deploying connector clients refer to [**Connector clients**](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).
You can get started quickly with Docker, using [these instructions](./docs/docker.md).
We also have an end-to-end tutorial using the [PostgreSQL connector client](https://www.elastic.co/guide/en/enterprise-search/current/postgresql-connector-client-tutorial.html).


## Python connector framework

This repo also contains the Elastic Python connector framework. This framework enables developers to build Elastic-supported connector clients.
The framework implements common functionalities out of the box, so developers can focus on the logic specific to integrating their chosen data source.

The framework ensures compatibility, makes it easier for our team to review PRs, and help out in the development process. When you build using our framework, we provide a pathway for the connector to be officially supported by Elastic.

### Framework use cases

The framework serves two distinct, but related use cases:

- Customizing an existing Elastic connector client
- Building a new connector client

## Guides

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

### Advanced sync rules

- [MySQL](docs/sync-rules/MYSQL.md)
- [JIRA](docs/sync-rules/JIRA.md)
- [CONFLUENCE](docs/sync-rules/CONFLUENCE.md)
- [SERVICENOW](docs/sync-rules/SERVICENOW.md)
- [GITHUB](docs/sync-rules/GITHUB.md)
- [NETWORK_DRIVE](docs/sync-rules/NETWORK_DRIVE.md)

### Document level security

- [JIRA](docs/document-level-security/JIRA.md)
- [CONFLUENCE](docs/document-level-security/CONFLUENCE.md)
