#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
import asyncio
import time
from datetime import datetime, timezone
from enum import Enum

from connectors.es import DEFAULT_LANGUAGE, ESIndex, defaults_for
from connectors.logger import logger
from connectors.source import DataSourceConfiguration, get_source_klass
from connectors.utils import iso_utc, next_run

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
PIPELINE = "ent-search-generic-ingestion"
SYNC_DISABLED = -1


def e2str(entry):
    return entry.name.lower()


class Status(Enum):
    CREATED = 1
    NEEDS_CONFIGURATION = 2
    CONFIGURED = 3
    CONNECTED = 4
    ERROR = 5


class JobStatus(Enum):
    PENDING = 1
    IN_PROGRESS = 2
    CANCELING = 3
    CANCELED = 4
    SUSPENDED = 5
    COMPLETED = 6
    ERROR = 7


class JobTriggerMethod(Enum):
    ON_DEMAND = 1
    SCHEDULED = 2


class ServiceTypeNotSupportedError(Exception):
    pass


class ServiceTypeNotConfiguredError(Exception):
    pass


class ConnectorUpdateError(Exception):
    pass


class DataSourceError(Exception):
    pass


class BYOIndex(ESIndex):
    CUSTOM_READ_ONLY_FIELDS = (
        "is_native",
        "api_key_id",
        "pipeline",
        "scheduling",
    )

    NATIVE_READ_ONLY_FIELDS = CUSTOM_READ_ONLY_FIELDS + (
        "service_type",
        "configuration",
    )

    def __init__(self, elastic_config):
        logger.debug(f"BYOIndex connecting to {elastic_config['host']}")
        # initilize ESIndex instance
        super().__init__(index_name=CONNECTORS_INDEX, elastic_config=elastic_config)
        # grab all bulk options
        self.bulk_options = elastic_config.get("bulk", {})
        self.language_code = elastic_config.get("language_code", DEFAULT_LANGUAGE)

    async def save(self, connector):
        # we never update the configuration
        document = dict(connector.doc_source)

        # read only we never update
        for key in (
            self.NATIVE_READ_ONLY_FIELDS if connector.native else self.CUSTOM_READ_ONLY_FIELDS
        ):
            if key in document:
                del document[key]

        return await self.client.update(
            index=CONNECTORS_INDEX,
            id=connector.id,
            doc=document,
        )

    # @TODO move to ESIndex?
    async def preflight(self):
        await self.check_exists(indices=[CONNECTORS_INDEX, JOBS_INDEX])

    def build_docs_query(self, native_service_types=None, connectors_ids=None):
        if native_service_types is None:
            native_service_types = []
        if connectors_ids is None:
            connectors_ids = []

        if len(native_service_types) == 0 and len(connectors_ids) == 0:
            return

        native_connectors_query = {
            "bool": {
                "filter": [
                    {"term": {"is_native": True}},
                    {"terms": {"service_type": native_service_types}},
                ]
            }
        }

        custom_connectors_query = {
            "bool": {
                "filter": [
                    {"term": {"is_native": False}},
                    {"terms": {"_id": connectors_ids}},
                ]
            }
        }
        if len(native_service_types) > 0 and len(connectors_ids) > 0:
            query = {
                "bool": {"should": [native_connectors_query, custom_connectors_query]}
            }
        elif len(native_service_types) > 0:
            query = native_connectors_query
        else:
            query = custom_connectors_query

        return query

    def _create_object(self, doc_source):
        return BYOConnector(
            self,
            doc_source["_id"],
            doc_source["_source"],
            bulk_options=self.bulk_options,
            language_code=self.language_code,
        )


class SyncJobIndex(ESIndex):
    READ_ONLY_FIELDS = (
        "connector",
        "trigger_method",
        "created_at"
    )

    def __init__(self, elastic_config):
        super().__init__(index_name=JOBS_INDEX, elastic_config=elastic_config)

    def build_docs_query(self, connector_ids):
        query = {"bool": {"must": [{"terms": {"status": [e2str(JobStatus.PENDING)]}}]}}

        if connector_ids:
            query["bool"]["must"].append({"terms": {"connector.id": connector_ids}})

        return query

    def _create_object(self, doc_source):
        return SyncJob(self, doc_source["_id"], doc_source["_source"])

    async def create(self, connector, trigger_method=JobTriggerMethod.SCHEDULED):
        job_def = {
            "connector": {
                "connector_id": connector.id,
                "configuration": connector.configuration,
                "index_name": connector.index_name,
                "service_type": connector.service_type,
                "language": connector.language_code,
                "pipeline": connector.pipeline,
            },
            "trigger_method": e2str(trigger_method),
            "status": e2str(JobStatus.PENDING),
            "error": None,
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "created_at": iso_utc(datetime.now(timezone.utc)),
            "completed_at": None,
        }

        resp = await self.client.index(index=JOBS_INDEX, document=job_def)
        self.job_id = resp["_id"]
        return self.job_id

    async def save(self, job):
        # we never update the configuration
        document = dict(job.doc_source)

        # read only we never update
        for key in self.READ_ONLY_FIELDS:
            if key in document:
                del document[key]

        return await self.client.update(
            index=JOBS_INDEX,
            id=job.id,
            doc=document,
        )


class SyncJob:
    def __init__(self, elastic_index, id, doc_source):

        print(f"[{id}]doc source:")
        print(doc_source)
        self.job_id = id
        self.doc_source = doc_source
        self.connector = doc_source["connector"]
        self.connector_id = doc_source["connector"]["connector_id"]
        self.completed_at = (
            datetime.fromisoformat(doc_source["completed_at"])
            if doc_source["completed_at"]
            else None
        )
        self.created_at = (
            datetime.fromisoformat(doc_source["created_at"])
            if doc_source["created_at"]
            else None
        )
        self.index = elastic_index

    @property
    def id(self):
        return self.job_id

    @property
    def connector_config(self):
        return self.connector

    @property
    def duration(self):
        if self.completed_at is None:
            return -1
        msec = (self.completed_at - self.created_at).microseconds
        return round(msec / 9, 2)

    async def start(self):
        self.status = JobStatus.IN_PROGRESS

        await self.sync_doc()

    async def done(self, indexed_count=0, deleted_count=0, exception=None):
        self.completed_at = datetime.now(timezone.utc)

        job_def = {
            "deleted_document_count": deleted_count,
            "indexed_document_count": indexed_count,
            "completed_at": iso_utc(self.completed_at),
        }

        if exception is None:
            self.status = JobStatus.COMPLETED
            job_def["error"] = None
        else:
            self.status = JobStatus.ERROR
            job_def["error"] = str(exception)

        job_def["status"] = e2str(self.status)

        return await self.sync_doc()

    async def sync_doc(self, force=True):
        self.doc_source["last_seen"] = iso_utc()
        await self.index.save(self)


class PipelineSettings:
    def __init__(self, pipeline):
        self.name = pipeline.get("name", "ent-search-generic-ingestion")
        self.extract_binary_content = pipeline.get("extract_binary_content", True)
        self.reduce_whitespace = pipeline.get("reduce_whitespace", True)
        self.run_ml_inference = pipeline.get("run_ml_inference", True)

    def __repr__(self):
        return (
            f"Pipeline {self.name} <binary: {self.extract_binary_content}, "
            f"whitespace {self.reduce_whitespace}, ml inference {self.run_ml_inference}>"
        )


class BYOConnector:
    """Represents one doc in `.elastic-connectors` and triggers sync.

    The pattern to use it is:

        await connector.prepare(config)
        await connector.start_heartbeat(delay)
        try:
            await connector.sync(es)
        finally:
            await connector.close()
    """

    def __init__(
        self,
        elastic_index,
        connector_id,
        doc_source,
        bulk_options,
        language_code=DEFAULT_LANGUAGE,
    ):

        self.doc_source = doc_source
        self.id = connector_id
        self.index = elastic_index
        self._update_config(doc_source)
        self._dirty = False
        self.client = elastic_index.client
        self.doc_source["last_seen"] = iso_utc()
        self._heartbeat_started = self._syncing = False
        self._closed = False
        self._start_time = None
        self._hb = None
        self.bulk_options = bulk_options
        self.language_code = language_code
        self.source_klass = None
        self.data_provider = None

    def _update_config(self, doc_source):
        self.status = doc_source["status"]
        self.sync_now = doc_source.get("sync_now", False)
        self.native = doc_source.get("is_native", False)
        self._service_type = doc_source["service_type"]
        self._index_name = doc_source["index_name"]
        self._configuration = doc_source["configuration"]
        self.scheduling = doc_source["scheduling"]
        self.pipeline = doc_source["pipeline"]
        self._dirty = True

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if isinstance(value, str):
            value = Status[value.upper()]
        if not isinstance(value, Status):
            raise TypeError(value)

        self._status = value
        self.doc_source["status"] = e2str(self._status)

    @property
    def index_name(self):
        return self._index_name

    @property
    def service_type(self):
        return self._service_type

    @service_type.setter
    def service_type(self, value):
        self._service_type = self.doc_source["service_type"] = value
        self._update_config(self.doc_source)

    @property
    def configuration(self):
        return self._configuration

    @configuration.setter
    def configuration(self, value):
        self.doc_source["configuration"] = value
        status = (
            Status.CONFIGURED
            if all(
                isinstance(value, dict) and value.get("value") is not None
                for value in value.values()
            )
            else Status.NEEDS_CONFIGURATION
        )
        self.doc_source["status"] = e2str(status)
        self._update_config(self.doc_source)

    async def close(self):
        self._closed = True
        if self._heartbeat_started:
            self._hb.cancel()
            self._heartbeat_started = False
        if self.data_provider is not None:
            await self.data_provider.close()
            self.data_provider = None

    async def sync_doc(self, force=True):
        if not self._dirty and not force:
            return
        self.doc_source["last_seen"] = iso_utc()
        await self.index.save(self)
        self._dirty = False

    async def heartbeat(self):
        logger.info(f"*** Connector {self.id} HEARTBEAT")
        self.doc_source["last_seen"] = iso_utc()
        await self.sync_doc()

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns SYNC_DISABLED, no sync is scheduled.
        """
        if self.sync_now:
            logger.debug("sync_now is true, syncing!")
            return 0
        if not self.scheduling["enabled"]:
            logger.debug("scheduler is disabled")
            return SYNC_DISABLED
        return next_run(self.scheduling["interval"])

    async def _sync_starts(self):
        job = SyncJob(self.id, self.client)
        trigger_method = (
            JobTriggerMethod.ON_DEMAND if self.sync_now else JobTriggerMethod.SCHEDULED
        )
        job_id = await job.start(trigger_method)

        self.sync_now = self.doc_source["sync_now"] = False
        self.doc_source["last_sync_status"] = e2str(job.status)
        self.status = Status.CONNECTED
        await self.sync_doc()

        self._start_time = time.time()
        logger.info(f"Sync starts, Job id: {job_id}")
        return job

    async def error(self, error):
        self.doc_source["error"] = str(error)
        await self.sync_doc()


    async def prepare(self, config):
        """Prepares the connector, given a configuration

        If the connector id and the service is in the config, we want to
        populate the service type and then sets the default configuration.

        Returns the source class.
        """
        configured_connector_id = config.get("connector_id", "")
        configured_service_type = config.get("service_type", "")

        if self.id == configured_connector_id and self.service_type is None:
            if not configured_service_type:
                logger.error(
                    f"Service type is not configured for connector {configured_connector_id}"
                )
                raise ServiceTypeNotConfiguredError("Service type is not configured.")
            self.service_type = configured_service_type
            logger.debug(f"Populated service type for connector {self.id}")

        service_type = self.service_type
        if service_type not in config["sources"]:
            raise ServiceTypeNotSupportedError(service_type)

        fqn = config["sources"][service_type]
        try:
            source_klass = get_source_klass(fqn)
            if self.configuration.is_empty():
                self.configuration = source_klass.get_default_configuration()
                logger.debug(f"Populated configuration for connector {self.id}")

            # sync state if needed (when service type or configuration is updated)
            try:
                await self.sync_doc(force=False)
            except Exception as e:
                logger.critical(e, exc_info=True)
                raise ConnectorUpdateError(
                    f"Could not update configuration for connector {self.id}"
                )
        except Exception as e:
            logger.critical(e, exc_info=True)
            raise DataSourceError(f"Could not instantiate {fqn} for {service_type}")

        self.source_klass = source_klass
