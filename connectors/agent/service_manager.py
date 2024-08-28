from es_agent_client.util.logger import logger

from connectors.logger import DocumentLogger
from connectors.services.base import ServiceAlreadyRunningError, get_services
from connectors.utils import CancellableSleeps


class ConnectorServiceManager:
    name = "connector-service-manager"

    def __init__(self, configuration):
        service_name = self.__class__.name
        self.logger = DocumentLogger(
            f"[{service_name}]", {"service_name": service_name}
        )
        self.agent_config = configuration
        self._multi_service = None
        self.running = False
        self._sleeps = CancellableSleeps()

    async def run(self):
        if self.running:
            msg = f"{self.__class__.__name__} is already running."
            raise ServiceAlreadyRunningError(msg)

        self.running = True
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
        finally:
            logger.info("Finished running, exiting")

    def stop(self):
        logger.info("Service manager is stopping connector services")
        self.running = False
        if self._multi_service:
            self._multi_service.shutdown(None)

    def restart(self):
        logger.info("Service manager is restarting connector services")
        if self._multi_service:
            self._multi_service.shutdown(None)
