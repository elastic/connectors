#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import re

import aiohttp

from connectors.es.management_client import ESManagementClient
from connectors.logger import logger
from connectors.protocol import CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX
from connectors.utils import CancellableSleeps


class PreflightCheck:
    def __init__(self, config, version):
        self.version = version
        self.config = config
        self.elastic_config = config["elasticsearch"]
        self.service_config = config["service"]
        self.extraction_config = config.get("extraction_service", None)
        self.es_management_client = ESManagementClient(self.elastic_config)
        self.preflight_max_attempts = int(
            self.service_config.get("preflight_max_attempts", 10)
        )
        self.preflight_idle = int(self.service_config.get("preflight_idle", 30))
        self._sleeps = CancellableSleeps()
        self.running = False

    def stop(self):
        self.running = False
        self._sleeps.cancel()
        if self.es_management_client is not None:
            self.es_management_client.stop_waiting()

    def shutdown(self, sig):
        logger.info(f"Caught {sig.name}. Graceful shutdown.")
        self.stop()

    async def run(self):
        try:
            logger.info("Running preflight checks")
            self.running = True

            success, is_serverless = await self._check_es_server()
            if not success:
                return False, is_serverless

            await self._check_local_extraction_setup()

            valid_configuration = self._validate_configuration()
            available_system_indices = await self._check_system_indices_with_retries()
            return (valid_configuration and available_system_indices), is_serverless
        finally:
            self.stop()
            if self.es_management_client is not None:
                await self.es_management_client.close()

    async def _check_es_server(self):
        """
        Returns two values:

        1. Whether or not the ES server is compatible with the running Connectors version
        2. Whether or not the ES server is serverless
        """

        es_info = await self.es_management_client.wait()
        if es_info is None:
            logger.critical(
                f"{self.elastic_config['host']} seems to be unreachable. Bye!"
            )
            return False, False

        version = es_info.get("version", {}) or {}
        is_serverless = version.get("build_flavor") == "serverless"
        if is_serverless:
            logger.info(
                "Elasticsearch server is serverless, skipping version compatibility check."
            )
            return True, is_serverless

        versions_compatible = await self._versions_compatible(version.get("number"))
        return versions_compatible, is_serverless

    async def _versions_compatible(self, es_version):
        """
        Checks if the Connector and ES versions are compatible
        """

        # Remove any version qualifier (e.g., -SNAPSHOT, -beta1, -rc1)
        major_minor_patch_es_version = re.split(r"-", es_version)[0]

        # array legend: 0 - major, 1 - minor, 2 - patch
        es_version_parts = list(map(int, major_minor_patch_es_version.split(".")))
        connector_version_parts = list(map(int, self.version.split("+")[0].split(".")))

        # major
        if es_version_parts[0] != connector_version_parts[0]:
            logger.critical(
                f"Elasticsearch {es_version} and Connectors {self.version} are incompatible: major versions are different"
            )
            return False

        # minor
        if es_version_parts[1] < connector_version_parts[1]:
            logger.critical(
                f"Elasticsearch {es_version} and Connectors {self.version} are incompatible: Elasticsearch minor version is older than Connectors"
            )
            return True

        if es_version_parts[1] > connector_version_parts[1]:
            logger.warning(
                f"Elasticsearch {es_version} minor version is newer than Connectors {self.version} which can lead to unexpected behavior"
            )
            return True

        # patch
        if es_version_parts[2] != connector_version_parts[2]:
            logger.warning(
                f"Elasticsearch {es_version} patch version is different than Connectors {self.version} which can lead to unexpected behavior"
            )
            return True

        logger.info(
            f"Elasticsearch {es_version} and Connectors {self.version} are compatible"
        )
        return True

    async def _check_local_extraction_setup(self):
        if self.extraction_config is None:
            logger.info(
                "Extraction service is not configured, skipping its preflight check."
            )
            return

        timeout = aiohttp.ClientTimeout(total=5)
        session = aiohttp.ClientSession(timeout=timeout)

        try:
            async with session.get(
                f"{self.extraction_config['host']}/ping/"
            ) as response:
                if response.status != 200:
                    logger.warning(
                        f"Data extraction service was found at {self.extraction_config['host']} but health-check returned `{response.status}'."
                    )
                else:
                    logger.info(
                        f"Data extraction service found at {self.extraction_config['host']}."
                    )
        except (aiohttp.ClientConnectionError, aiohttp.ServerTimeoutError) as e:
            logger.critical(
                f"Expected to find a running instance of data extraction service at {self.extraction_config['host']} but failed. {e}."
            )
        except Exception as e:
            logger.critical(
                f"Unexpected error occurred while attempting to connect to data extraction service at {self.extraction_config['host']}. {e}."
            )
        finally:
            await session.close()

    async def _check_system_indices_with_retries(self):
        attempts = 0
        while self.running:
            try:
                # Checking the indices/pipeline in the loop to be less strict about the boot ordering
                # Using concrete write index to create these to ensure ES-installed template processes
                # The templates are installed here: https://github.com/elastic/elasticsearch/blob/main/x-pack/plugin/ent-search/src/main/java/org/elasticsearch/xpack/application/connector/ConnectorTemplateRegistry.java
                # and located here: https://github.com/elastic/elasticsearch/tree/main/x-pack/plugin/core/template-resources/src/main/resources/entsearch/connector

                await self.es_management_client.ensure_exists(
                    indices=[CONCRETE_CONNECTORS_INDEX, CONCRETE_JOBS_INDEX],
                )
                return True
            except Exception as e:
                if attempts >= self.preflight_max_attempts:
                    logger.critical(
                        f"Could not perform preflight check after {self.preflight_max_attempts} retries."
                    )
                    return False
                else:
                    logger.warning(
                        f"Attempt {attempts + 1}/{self.preflight_max_attempts} failed. Retrying..."
                    )
                    logger.warning(str(e))
                    attempts += 1
                    await self._sleeps.sleep(self.preflight_idle)
        return False

    def _validate_configuration(self):
        # "Native" mode
        configured_native_types = "native_service_types" in self.config
        force_allowed_native = self.config.get("_force_allow_native", False)
        if configured_native_types and not force_allowed_native:
            logger.warning(
                "The configuration 'native_service_types' has been deprecated. Please remove this configuration."
            )
            logger.warning(
                "Native Connectors are only supported internal to Elastic Cloud deployments, which this process is not."
            )

        # Connector client mode
        configured_connectors = self.config.get("connectors", []) or []
        deprecated_connector_id = self.config.get("connector_id", None)
        deprecated_service_type = self.config.get("service_type", None)
        if (
            not configured_connectors
            and deprecated_connector_id
            and deprecated_service_type
        ):
            logger.warning(
                "The configuration 'connector_id' and 'serivce_type' has been deprecated and will be removed in later release. Please configure the connector in 'connectors'."
            )
            configured_connectors.append(
                {
                    "connector_id": deprecated_connector_id,
                    "service_type": deprecated_service_type,
                }
            )

        if not configured_connectors and not force_allowed_native:
            logger.warning(
                "Please update your config.yml to configure at least one connector"
            )
            logger.info(
                "Using Kibana or the connectors CLI, create a connector. You will then be provided with the necessary fields (connector_id, service_type, api_key) to add to your config.yml"
            )

        # Unset configuration
        if not configured_native_types and not configured_connectors:
            logger.error("You must configure at least one connector. ")
            return False

        # Default configuration
        for connector in configured_connectors:
            configured_connector_id = connector.get("connector_id", None)
            configured_service_type = connector.get("service_type", None)
            if (
                configured_connector_id == "changeme"
                or configured_service_type == "changeme"
            ):
                logger.error("Unmodified default configuration detected.")
                logger.error(
                    "In your configuration, you must change 'connector_id' and 'service_type' to not be 'changeme'"
                )
                return False

        # if we made it here, we didn't hit any critical issues
        return True
