#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Plugin registry for sources classes.
"""
import importlib
from connectors.logger import logger


def get_klass(fqn):
    """Converts a Fully Qualified Name into a class instance."""
    module_name, klass_name = fqn.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


def get_data_provider(definition, config):
    """Returns a source class instance, given a service type"""
    service_type = definition.service_type
    logger.debug(f"Getting source instance for {service_type}")
    klass = get_klass(config["sources"][service_type])
    logger.debug(f"Found a matching plugin {klass}")
    return klass(definition)


def get_data_providers(config):
    """Returns an iterator of all registered sources."""
    for name, fqn in config["sources"].items():
        yield get_klass(fqn)
