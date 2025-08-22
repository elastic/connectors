# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Installation and Setup
- `make install` - Install dependencies and set up the development environment
- `make clean` - Clean build artifacts and virtual environment
- `make clean install` - Clean install from scratch

### Code Quality
- `make lint` - Run linting with ruff and type checking with pyright
- `make autoformat` - Auto-format code with ruff (runs the linter as part of this, and fixes what it can)
- `make test` - Run unit tests with pytest (requires 92% coverage)

### Testing
- `make test` - Run unit tests with coverage reporting
- `make ftest NAME={service_type}` - Run functional tests for a specific connector
- `make ftrace NAME={service_type}` - Run functional tests with performance tracing

### Running the Service
- `make run` - Run the connector service in debug mode
- `.venv/bin/elastic-ingest --help` - View CLI options
- `make default-config SERVICE_TYPE={type}` - Generate default configuration

### Docker Operations
- `make docker-build` - Build Docker image
- `make docker-run` - Run Docker container
- `make agent-docker-build` - Build agent Docker image
- `make agent-docker-run` - Run agent Docker container

## Architecture Overview

This is the Elastic Connectors framework - a Python-based system for syncing data from various sources into Elasticsearch.

### Core Components

**Service Layer (`connectors/service_cli.py`)**
- Main entry point via `elastic-ingest` command
- Runs an asynchronous event loop that polls Elasticsearch for sync jobs
- Follows the Connector Protocol for communication with Kibana

**Source Framework (`connectors/source.py`)**
- `BaseDataSource` - Base class for all connector implementations
- Rich Configurable Fields (RCF) system for dynamic UI generation in Kibana
- Support for full sync, incremental sync, and document-level security

**Configuration (`connectors/config.py`)**
- YAML-based configuration with environment variable support
- Sources registry mapping connector names to their Python classes
- Default Elasticsearch connection and bulk operation settings

### Connector Sources

All connector implementations are in `connectors/sources/` and registered in `config.py`:
- MongoDB, MySQL, PostgreSQL, Oracle, MSSQL, Redis (databases)
- SharePoint Online/Server, OneDrive, Box, Dropbox, Google Drive (file storage)
- Jira, Confluence, ServiceNow, Salesforce, Slack, Teams (SaaS platforms)
- S3, Azure Blob Storage, Google Cloud Storage (cloud storage)
- GitHub, Notion, Gmail, Outlook, Zoom (other platforms)

### Key Patterns

**Async Architecture**
- All connector code must be non-blocking using asyncio
- Use `async`/`await` for I/O operations
- For CPU-bound work, use `run_in_executor` with threads/processes

**Connector Implementation**
- Extend `BaseDataSource` class
- Implement `get_docs()` for full sync, `get_docs_incrementally()` for incremental sync
- Define `get_default_configuration()` for Rich Configurable Fields
- Use `self._logger` for logging to include sync job context

**Document Processing**
- Documents yielded as tuples: `(document, lazy_download, operation)`
- Operations: `index`, `update`, `delete`
- Content extraction via Tika for binary files
- Document IDs stored in memory for deletion detection

**Sync Rules**
- Basic rules: framework-level filtering (enabled by default)
- Advanced rules: connector-specific filtering (implement `advanced_rules_validators()`)
- Document-level security via `_allow_permissions` field

## Testing Requirements

### Unit Tests
- Located in `tests/` directory
- Minimum 92% test coverage required
- Run with `make test`
- Mock external services and APIs

### Functional Tests  
- Located in `tests/fixtures/` for each connector
- Requires Docker containers or real service credentials
- Must return >10k documents to test pagination
- Run with `make ftest NAME={connector_name}`
- Includes performance monitoring and memory usage analysis

### Test Configuration
- `pytest.ini` configures async mode and warning filters
- `pyrightconfig.json` sets up type checking for Python 3.10+
- Coverage reports generated in HTML format

## Development Guidelines

**Adding New Connectors**
1. Create connector class in `connectors/sources/{name}.py`
2. Add mapping in `connectors/config.py` sources section  
3. Create unit tests in `tests/sources/test_{name}.py`
4. Add dependencies to `requirements/framework.txt` (pinned versions)
5. Provide Docker test environment in `tests/fixtures/{name}/`
6. Implement required methods from `BaseDataSource`

**Dependencies**
- Python 3.10+ required
- Install architecture-specific requirements: `requirements/{arch}.txt`
- Pin all dependency versions
- Document licenses for new dependencies

**Configuration**
- Copy `config.yml.example` to `config.yml` for local development
- Environment variables supported via EnvYAML
- Elasticsearch connection defaults to `localhost:9200` with `elastic/changeme`