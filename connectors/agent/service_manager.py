from es_agent_client.util.logger import logger

from connectors.services.base import BaseService, get_services


class ConnectorServiceManager(BaseService):
    name = "connector-service-manager"

    def __init__(self, configuration):
        super().__init__(configuration.get(), "connector-service-manager")
        self.agent_config = configuration
        self._multi_service = None

    async def _run(self):
        try:
            while self.running:
                try:
                    logger.info("Starting connector services")
                    self._multi_service = get_services(
                        ["schedule", "sync_content", "sync_access_control", "cleanup"],
                        self.agent_config.get(),
                    )
                    await self._multi_service.run()
                except Exception as e:
                    logger.exception(
                        f"Error while running services in ConnectorServiceManager: {e}"
                    )
                    raise
                if not self.running:
                    break
                await self._sleeps.sleep(1)
        finally:
            logger.info("Finished running, exiting")
        return 0

    def stop(self):
        logger.info("Service manager is stopping connector services")
        super().stop()
        if self._multi_service:
            self._multi_service.shutdown(None)

    def restart(self):
        logger.info("Service manager is restarting connector services")
        if self._multi_service:
            self._multi_service.shutdown(None)
