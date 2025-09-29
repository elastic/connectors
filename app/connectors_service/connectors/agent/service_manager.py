#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import logging

import connectors_sdk.logger

import connectors.agent.logger
from connectors.agent.logger import get_logger
from connectors.services.base import (
    ServiceAlreadyRunningError,
    get_services,
)
from connectors.utils import CancellableSleeps

logger = get_logger("service_manager")


class ConnectorServiceManager:
    """Class responsible for properly configuring and running Connectors Service in Elastic Agent

    ConnectorServiceManager is a middle man between Elastic Agent and ConnectorsService.

    This class is taking care of starting Connectors Service subservices with correct configuration.
    If configuration changes, as reported by Agent, then this Manager class gracefully shuts down the
    subservices and starts them again with new configuration.

    """

    def __init__(self, configuration):
        """Inits ConnectorServiceManager with shared ConnectorsAgentConfigurationWrapper.

        This service is supposed to be ran once, and after it's stopped or finished running it's not
        supposed to be started again.

        There is nothing enforcing it, but expect problems if that happens.
        """
        self._agent_config = configuration
        self._multi_service = None
        self._running = False
        self._sleeps = CancellableSleeps()

    async def run(self):
        """Starts the running loop of the service.

        Once started, the service attempts to run all needed connector subservices
        in parallel via MultiService.

        Service can be restarted - it will keep running but with refreshed config,
        or it can be stopped - it will just gracefully shut down.
        """
        if self._running:
            msg = f"{self.__class__.__name__} is already running."
            raise ServiceAlreadyRunningError(msg)

        self._running = True

        try:
            while self._running:
                try:
                    logger.info("Starting connector services")
                    config = self._agent_config.get()
                    self._multi_service = get_services(
                        ["schedule", "sync_content", "sync_access_control", "cleanup"],
                        config,
                    )
                    log_level = config.get("service", {}).get(
                        "log_level", logging.INFO
                    )  # Log Level for connectors is managed like this
                    connectors_sdk.logger.set_logger(log_level, filebeat=True)
                    # Log Level for agent connectors component itself
                    connectors.agent.logger.update_logger_level(log_level)

                    await self._multi_service.run()
                except Exception as e:
                    logger.exception(
                        f"Error while running services in ConnectorServiceManager: {e}"
                    )
                    raise
        finally:
            logger.info("Finished running, exiting")

    def stop(self):
        """Stop the service manager and all running subservices.

        Running stop attempts to gracefully shutdown all subservices currently running.
        """
        logger.info("Stopping connector services.")
        self._running = False
        self._done = True
        if self._multi_service:
            self._multi_service.shutdown(None)

    def restart(self):
        """Restart the service manager and all running subservices.

        Running restart attempts to gracefully shutdown all subservices currently running.
        After services are gracefully stopped, they will be started again with fresh configuration
        that comes from ConnectorsAgentConfigurationWrapper.
        """
        logger.info("Restarting connector services")
        if self._multi_service:
            self._multi_service.shutdown(None)
