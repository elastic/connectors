#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

"""
Example plugin for Elasticsearch Connectors Service.

This plugin demonstrates how to extend the connectors service with
additional data sources via the entry-points mechanism.
"""

__version__ = "0.1.0"

from typing import Dict


def external_sources() -> Dict[str, str]:
    """
    Entry point function for the connectors_service.external_sources plugin.

    This function is called by the connectors service to load external source
    configurations. It should return a dictionary that will be merged with the
    default configuration.

    Returns:
        dict: Configuration dictionary with the same structure as the default
              config in connectors.config._default_config()
    """
    return {
        "my_custom_source": "connectors_plugin_example.datasource:CustomDirectoryDataSource",
    }