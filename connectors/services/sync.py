#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instantiates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import asyncio
import functools

from connectors.byoc import (
    ConnectorIndex,
    ConnectorUpdateError,
    DataSourceError,
    ServiceTypeNotConfiguredError,
    ServiceTypeNotSupportedError,
    Status,
    SyncJobIndex,
)
from connectors.byoei import ElasticServer
from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.source import get_source_klass_dict
from connectors.utils import ConcurrentTasks

DEFAULT_MAX_CONCURRENT_SYNCS = 1


class SyncService(BaseService):
    def __init__(self, config):
        super().__init__(config)
        self.idling = self.service_config["idling"]
        self.hb = self.service_config["heartbeat"]
        self.concurrent_syncs = self.service_config.get(
            "max_concurrent_syncs", DEFAULT_MAX_CONCURRENT_SYNCS
        )
        self.bulk_options = self.es_config.get("bulk", {})
        self.source_klass_dict = get_source_klass_dict(config)
        self.connectors = None
        self.sync_job_index = None
        self.syncs = None

    def stop(self):
        super().stop()
        if self.syncs is not None:
            self.syncs.cancel()

    async def _one_sync(self, connector, es):
        if self.running is False:
            logger.debug(
                f"Skipping run for {connector.id} because service is terminating"
            )
            return

        if connector.native:
            logger.debug(f"Connector {connector.id} natively supported")

        try:
            await connector.prepare(self.config)
        except ServiceTypeNotConfiguredError:
            logger.error(
                f"Service type is not configured for connector {self.config['connector_id']}"
            )
            return
        except ConnectorUpdateError as e:
            logger.error(e)
            return
        except ServiceTypeNotSupportedError:
            logger.debug(f"Can't handle source of type {connector.service_type}")
            return
        except DataSourceError as e:
            await connector.error(e)
            logger.critical(e, exc_info=True)
            self.raise_if_spurious(e)
            return

        # the heartbeat is always triggered
        await connector.heartbeat(self.hb)

        logger.debug(f"Connector status is {connector.status}")

        # we trigger a sync
        if connector.status in (Status.CREATED, Status.NEEDS_CONFIGURATION):
            # we can't sync in that state
            logger.info(f"Can't sync with status `{connector.status.value}`")
        else:
            if connector.service_type not in self.source_klass_dict:
                raise DataSourceError(
                    f"Couldn't find data source class for {connector.service_type}"
                )
            source_klass = self.source_klass_dict[connector.service_type]
            if connector.features.sync_rules_enabled():
                await connector.validate_filtering(
                    validator=source_klass(connector.configuration)
                )

            await connector.sync(
                self.sync_job_index,
                source_klass,
                es,
                self.idling,
                self.bulk_options,
            )

        await asyncio.sleep(0)

    async def _run(self):
        """Main event loop."""
        self.connectors = ConnectorIndex(self.es_config)
        self.sync_job_index = SyncJobIndex(self.es_config)

        native_service_types = self.config.get("native_service_types", [])
        logger.debug(f"Native support for {', '.join(native_service_types)}")

        # XXX we can support multiple connectors but Ruby can't so let's use a
        # single id
        # connector_ids = self.config.get("connector_ids", [])
        if "connector_id" in self.config:
            connector_ids = [self.config.get("connector_id")]
        else:
            connector_ids = []

        logger.info(
            f"Service started, listening to events from {self.es_config['host']}"
        )

        es = ElasticServer(self.es_config)
        try:
            while self.running:
                # creating a pool of task for every round
                self.syncs = ConcurrentTasks(max_concurrency=self.concurrent_syncs)

                try:
                    logger.debug(f"Polling every {self.idling} seconds")
                    async for connector in self.connectors.supported_connectors(
                        native_service_types=native_service_types,
                        connector_ids=connector_ids,
                    ):
                        await self.syncs.put(
                            functools.partial(self._one_sync, connector, es)
                        )
                except Exception as e:
                    logger.critical(e, exc_info=True)
                    self.raise_if_spurious(e)
                finally:
                    await self.syncs.join()

                self.syncs = None
                # Immediately break instead of sleeping
                if not self.running:
                    break
                await self._sleeps.sleep(self.idling)
        finally:
            if self.connectors is not None:
                self.connectors.stop_waiting()
                await self.connectors.close()
            if self.sync_job_index is not None:
                self.sync_job_index.stop_waiting()
                await self.sync_job_index.close()
            await es.close()
        return 0
