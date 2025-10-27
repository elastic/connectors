# Connectors service

The connectors service is what powers the Elastic Connector experience. It has handles the connection to Elasticsearch, content syncs, scheduling and final cleanup.

The source code implementations for individual data sources also live here. If you are looking to contribute a new data source implementation, this is the place to write it.

In this folder you will find:
- A `pyproject.toml` file
- Connectors service definition and its entry points under `connectors/`
- The source code implementation for individual data sources under `connectors/sources/`
- Relevant testing code and fixtures under `tests/`
