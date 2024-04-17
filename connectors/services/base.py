#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Provides:

- `BaseService`: a base class for running a service in the CLI
- `MultiService`: a meta-service that runs several services against the same
  config
- `get_services` and `get_service`: factories
"""
import asyncio
import time
from copy import deepcopy

from connectors.logger import DocumentLogger, logger
from connectors.utils import CancellableSleeps

__all__ = [
    "MultiService",
    "ServiceAlreadyRunningError",
    "get_service",
    "get_services",
    "BaseService",
]


class ServiceAlreadyRunningError(Exception):
    pass


_SERVICES = {}


def get_services(names, config):
    """Instantiates a list of services given their names and a config.

    returns a `MultiService` instance.
    """
    return MultiService(*[get_service(name, config) for name in names])


def get_service(name, config):
    """Instantiates a service object given a name and a config"""
    return _SERVICES[name](config)


class _Registry(type):
    """Metaclass used to register a service class in an internal registry."""

    def __new__(cls, name, bases, dct):
        service_name = dct.get("name")
        class_instance = super().__new__(cls, name, bases, dct)
        if service_name is not None:
            _SERVICES[service_name] = class_instance
        return class_instance


class BaseService(metaclass=_Registry):
    """Base class for creating a service.

    Any class deriving from this class will get added to the registry,
    given its `name` class attribute (unless it's not set).

    A concrete service class needs to implement `_run`.
    """

    name = None  # using None here avoids registering this class

    def __init__(self, config, service_name):
        self.config = config
        self.service_config = self.config["service"]
        self.es_config = self.config["elasticsearch"]
        self.connectors = self._parse_connectors()
        self.connector_index = None
        self.sync_job_index = None
        self.running = False
        self._sleeps = CancellableSleeps()
        self.errors = [0, time.time()]
        self.logger = DocumentLogger(f"[{service_name}]", { 'service_name': service_name })

    def stop(self):
        self.running = False
        self._sleeps.cancel()

    async def _run(self):
        raise NotImplementedError()

    async def run(self):
        """Runs the service"""
        if self.running:
            msg = f"{self.__class__.__name__} is already running."
            raise ServiceAlreadyRunningError(msg)

        self.running = True
        try:
            await self._run()
        finally:
            self.stop()

    def raise_if_spurious(self, exception):
        errors, first = self.errors
        errors += 1

        # if we piled up too many errors we raise and quit
        if errors > self.service_config["max_errors"]:
            raise exception

        # we re-init every ten minutes
        if time.time() - first > self.service_config["max_errors_span"]:
            first = time.time()
            errors = 0

        self.errors[0] = errors
        self.errors[1] = first

    def _parse_connectors(self):
        connectors = {}
        configured_connectors = deepcopy(self.config.get("connectors"))
        if configured_connectors is not None:
            for connector in configured_connectors:
                connector_id = connector.get("connector_id")
                if not connector_id:
                    self.logger.warning(
                        f"Found invalid connector configuration. Connector id is missing for {connector}"
                    )
                    continue

                connector_id = str(connector_id)
                if connector_id in connectors:
                    self.logger.warning(
                        f"Found duplicate configuration for connector {connector_id}, overriding with the later config"
                    )
                connectors[connector_id] = connector

        if not connectors:
            if "connector_id" in self.config and "service_type" in self.config:
                connector_id = str(self.config["connector_id"])
                connectors[connector_id] = {
                    "connector_id": connector_id,
                    "service_type": self.config["service_type"],
                }

        return connectors

    def _override_es_config(self, connector):
        es_config = deepcopy(self.es_config)
        if connector.id not in self.connectors:
            return es_config

        api_key = self.connectors[connector.id].get("api_key", None)
        if not api_key:
            return es_config

        es_config.pop("username", None)
        es_config.pop("password", None)
        es_config["api_key"] = api_key

        return es_config


class MultiService:
    """Wrapper class to run multiple services against the same config."""

    def __init__(self, *services):
        self._services = services

    async def run(self):
        """Runs every service in a task and wait for all tasks."""
        tasks = [asyncio.create_task(service.run()) for service in self._services]

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                logger.error("Service did not handle cancellation gracefully.")

    def shutdown(self, sig):
        logger.info(f"Caught {sig}. Graceful shutdown.")

        for service in self._services:
            logger.debug(f"Shutting down {service.__class__.__name__}...")
            service.stop()
            logger.debug(f"Done shutting down {service.__class__.__name__}...")
