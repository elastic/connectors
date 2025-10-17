# Elasticsearch Connectors Plugin Example

This is an example plugin package for the Elasticsearch Connectors Service that demonstrates how to extend the connectors framework with custom data sources using the entry-points mechanism.

## Installation

Install in development mode:

```bash
pip install -e .
```

## Usage

This plugin uses Python's entry-points mechanism to register itself with the `connectors_service` package. The entry point is defined in `pyproject.toml`:

```toml
[project.entry-points."connectors_service.external_sources"]
example_plugin = "connectors_plugin_example:external_sources"
```

The `external_sources()` function in `__init__.py` returns a configuration dictionary that gets merged with the default connectors configuration, allowing you to add custom data source mappings.

## Adding Custom Data Sources

To add a custom data source:

1. Implement your data source class (should inherit from the appropriate base class in `connectors_sdk`)
2. Add the mapping to the dictionary returned by `external_sources()`:

```python
def external_sources():
    return {
        "sources": {
            "my_custom_source": "connectors_plugin_example.sources.custom:CustomDataSource",
        }
    }
```

## Structure

- `connectors_plugin_example/__init__.py` - Main plugin module with entry-point function
- `connectors_plugin_example/VERSION` - Version file
- `pyproject.toml` - Package configuration with entry-points definition
- `README.md` - This file