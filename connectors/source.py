#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" Helpers to build sources + FQN-based Registry
"""
import importlib
from connectors.logger import logger


class ServiceTypeNotSupportedError(Exception):
    pass


class ServiceTypeNotConfiguredError(Exception):
    pass


class ConnectorUpdateError(Exception):
    pass


class DataSourceError(Exception):
    pass


class Field:
    def __init__(self, name, label=None, value="", type="str"):
        if label is None:
            label = name
        self.name = name
        self.label = label
        self.value = value
        self.type = type


class DataSourceConfiguration:
    """Holds the configuration needed by the source class"""

    def __init__(self, config):
        self._config = {}
        for key, value in config.items():
            self.set_field(
                key,
                value.get("label"),
                value.get("value", ""),
                value.get("type", "str"),
            )

    def __getitem__(self, key):
        return self._config[key].value

    def get(self, key, default=None):
        if key not in self._config:
            return default
        return self._config[key].value

    def has_field(self, name):
        return name in self._config

    def set_field(self, name, label=None, value="", type="str"):
        self._config[name] = Field(name, label, value, type)

    def get_field(self, name):
        return self._config[name]

    def get_fields(self):
        return self._config.values()

    def is_empty(self):
        return len(self._config) == 0


class BaseDataSource:
    """Base class, defines a lose contract."""

    def __init__(self, connector):
        self.connector = connector
        self.configuration = connector.configuration

    def __str__(self):
        return f"Datasource `{self.__class__.__doc__}`"

    @classmethod
    def get_default_configuration(cls):
        """Returns a dict with a default configuration"""
        raise NotImplementedError

    async def changed(self):
        """When called, returns True if something has changed in the backend.

        Otherwise returns False and the next sync is skipped.

        Some backends don't provide that information.
        In that case, this always return True.
        """
        return True

    async def ping(self):
        """When called, pings the backend

        If the backend has an issue, raises an exception
        """
        raise NotImplementedError

    async def close(self):
        """Called when the source is closed.

        Can be used to close connections
        """
        pass

    async def get_docs(self):
        """Returns an iterator on all documents present in the backend

        Each document is a tuple with:
        - a mapping with the data to index
        - a coroutine to download extra data (attachments)

        The mapping should have least an `id` field
        and optionally a `timestamp` field in ISO 8601 UTC

        The coroutine is called if the document needs to be synced
        and has attachements. It need to return a mapping to index.

        It has two arguments: doit and timestamp
        If doit is False, it should return None immediatly.
        If timestamp is provided, it should be used in the mapping.

        Example:

           async def get_file(doit=True, timestamp=None):
               if not doit:
                   return
               return {'TEXT': 'DATA', 'timestamp': timestamp,
                       'id': 'doc-id'}
        """
        raise NotImplementedError


def get_source_klass(fqn):
    """Converts a Fully Qualified Name into a class instance."""
    module_name, klass_name = fqn.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, klass_name)


async def get_data_source(connector, config):
    """Returns a source class instance, given a service type"""
    configured_connector_id = config.get("connector_id", "")
    configured_service_type = config.get("service_type", "")
    if connector.id == configured_connector_id and connector.service_type is None:
        if not configured_service_type:
            logger.error(
                f"Service type is not configured for connector {configured_connector_id}"
            )
            raise ServiceTypeNotConfiguredError("Service type is not configured.")
        try:
            await connector.populate_service_type(configured_service_type)
            logger.debug(
                f"Populated service type {configured_service_type} for connector {connector.id}"
            )
        except Exception as e:
            logger.critical(e, exc_info=True)
            raise ConnectorUpdateError(
                f"Could not update service type for connector {connector.id}"
            )
    service_type = connector.service_type
    if service_type not in config["sources"]:
        raise ServiceTypeNotSupportedError(service_type)

    fqn = config["sources"][service_type]
    try:
        source_klass = get_source_klass(fqn)
        if connector.configuration.is_empty():
            try:
                await connector.populate_configuration(
                    source_klass.get_default_configuration()
                )
                logger.debug(f"Populated configuration for connector {connector.id}")
            except Exception as e:
                logger.critical(e, exc_info=True)
                raise ConnectorUpdateError(
                    f"Could not update configuration for connector {connector.id}"
                )
        return source_klass(connector)
    except Exception as e:
        logger.critical(e, exc_info=True)
        raise DataSourceError(f"Could not instantiate {fqn} for {service_type}")


def get_data_sources(config):
    """Returns an iterator of all registered sources."""
    for name, fqn in config["sources"].items():
        yield get_source_klass(fqn)
