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
from copy import deepcopy
from datetime import datetime, timezone
from enum import Enum

from connectors.es import DEFAULT_LANGUAGE, ESIndex, Mappings
from connectors.filtering.validation import ValidationTarget, validate_filtering
from connectors.logger import logger
from connectors.source import DataSourceConfiguration, get_source_klass
from connectors.utils import e2str, iso_utc, next_run, str2e

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
PIPELINE = "ent-search-generic-ingestion"
RETRY_ON_CONFLICT = 3
SYNC_DISABLED = -1


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


class ServiceTypeNotSupportedError(Exception):
    pass


class ServiceTypeNotConfiguredError(Exception):
    pass


class ConnectorUpdateError(Exception):
    pass


class DataSourceError(Exception):
    pass


class ConnectorIndex(ESIndex):
    def __init__(self, elastic_config):
        logger.debug(f"ConnectorIndex connecting to {elastic_config['host']}")
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
            NATIVE_READ_ONLY_FIELDS if connector.native else CUSTOM_READ_ONLY_FIELDS
        ):
            if key in document:
                del document[key]

        return await self.client.update(
            index=CONNECTORS_INDEX,
            id=connector.id,
            doc=document,
        )

    async def update_filtering_validation(
        self, connector, validation_result, validation_target=ValidationTarget.ACTIVE
    ):
        doc_to_update = deepcopy(connector.doc_source)

        for filter_ in doc_to_update.get("filtering", []):
            if filter_.get("domain", "") == Filtering.DEFAULT_DOMAIN:
                filter_.get(e2str(validation_target), {"validation": {}})[
                    "validation"
                ] = validation_result.to_dict()

        await self.client.update(
            index=CONNECTORS_INDEX,
            id=connector.id,
            doc=doc_to_update,
            retry_on_conflict=RETRY_ON_CONFLICT,
        )

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

    async def supported_connectors(
        self, native_service_types=None, connectors_ids=None
    ):
        query = self.build_docs_query(native_service_types, connectors_ids)
        if query is None:
            return

        async for connector in self.get_all_docs(query=query):
            yield connector

    def _create_object(self, doc_source):
        return Connector(
            self,
            doc_source["_id"],
            doc_source["_source"],
            bulk_options=self.bulk_options,
        )

    async def all_connectors(self):
        async for connector in self.get_all_docs():
            yield connector


class SyncJob:
    def __init__(self, elastic_index, connector_id, doc_source=None):
        self.connector_id = connector_id
        self.elastic_index = elastic_index
        self.created_at = datetime.now(timezone.utc)
        self.completed_at = None
        self.filtering = Filter()
        self.client = elastic_index.client
        if doc_source is None:
            doc_source = dict()

        self.doc_source = doc_source
        self.job_id = self.doc_source.get("_id")
        self.status = str2e(self.doc_source.get("_source", {}).get("status"), JobStatus)

    @property
    def duration(self):
        if self.completed_at is None:
            return -1
        msec = (self.completed_at - self.created_at).microseconds
        return round(msec / 9, 2)

    @property
    def index_name(self):
        return self.doc_source["_source"]["connector"]["index_name"]

    async def start(self, trigger_method=JobTriggerMethod.SCHEDULED, filtering=None):
        if filtering is None:
            filtering = Filter()

        self.status = JobStatus.IN_PROGRESS
        self.filtering = filtering

        job_def = {
            "connector": {
                "id": self.connector_id,
                "filtering": SyncJob.transform_filtering(filtering),
            },
            "trigger_method": e2str(trigger_method),
            "status": e2str(self.status),
            "error": None,
            "deleted_document_count": 0,
            "indexed_document_count": 0,
            "created_at": iso_utc(self.created_at),
            "completed_at": None,
        }
        resp = await self.client.index(index=JOBS_INDEX, document=job_def)
        self.job_id = resp["_id"]
        return self.job_id

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

        return await self.client.update(index=JOBS_INDEX, id=self.job_id, doc=job_def)

    async def suspend(self):
        self.status = JobStatus.SUSPENDED
        job_def = {"status": e2str(self.status)}

        await self.client.update(index=JOBS_INDEX, id=self.job_id, doc=job_def)

    @classmethod
    def transform_filtering(cls, filtering):
        # deepcopy to not change the reference resulting in changing .elastic-connectors filtering
        filtering = (
            {"advanced_snippet": {}, "rules": []}
            if (filtering is None or len(filtering) == 0)
            else deepcopy(filtering)
        )

        # extract value for sync job
        filtering["advanced_snippet"] = filtering.get("advanced_snippet", {}).get(
            "value", {}
        )
        return filtering


class Filtering:
    DEFAULT_DOMAIN = "DEFAULT"

    def __init__(self, filtering=None):
        if filtering is None:
            filtering = []

        self.filtering = filtering

    def get_active_filter(self, domain=DEFAULT_DOMAIN):
        return self.get_filter(filter_state="active", domain=domain)

    def get_draft_filter(self, domain=DEFAULT_DOMAIN):
        return self.get_filter(filter_state="draft", domain=domain)

    def get_filter(self, filter_state="active", domain=DEFAULT_DOMAIN):
        return next(
            (
                Filter(filter_[filter_state])
                for filter_ in self.filtering
                if filter_["domain"] == domain
            ),
            Filter(),
        )


class Filter(dict):
    def __init__(self, filter_=None):
        if filter_ is None:
            filter_ = {}

        super().__init__(filter_)

        advanced_rules = filter_.get("advanced_snippet", {})

        self.advanced_rules = advanced_rules.get("value", advanced_rules)
        self.basic_rules = filter_.get("rules", [])
        self.validation = filter_.get("validation", {})

    def get_advanced_rules(self):
        return self.advanced_rules

    def has_advanced_rules(self):
        return len(self.advanced_rules) > 0


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


class Features:
    BASIC_RULES_NEW = "basic_rules_new"
    ADVANCED_RULES_NEW = "advanced_rules_new"

    # keep backwards compatibility
    BASIC_RULES_OLD = "basic_rules_old"
    ADVANCED_RULES_OLD = "advanced_rules_old"

    def __init__(self, features=None):
        if features is None:
            features = {}

        self.features = features

    def sync_rules_enabled(self):
        return any(
            [
                self.feature_enabled(Features.BASIC_RULES_NEW),
                self.feature_enabled(Features.BASIC_RULES_OLD),
                self.feature_enabled(Features.ADVANCED_RULES_NEW),
                self.feature_enabled(Features.ADVANCED_RULES_OLD),
            ]
        )

    def feature_enabled(self, feature):
        match feature:
            case Features.BASIC_RULES_NEW:
                return self._nested_feature_enabled(
                    ["sync_rules", "basic", "enabled"], default=False
                )
            case Features.ADVANCED_RULES_NEW:
                return self._nested_feature_enabled(
                    ["sync_rules", "advanced", "enabled"], default=False
                )
            case Features.BASIC_RULES_OLD:
                return self.features.get("filtering_rules", False)
            case Features.ADVANCED_RULES_OLD:
                return self.features.get("filtering_advanced_config", False)
            case _:
                return False

    def _nested_feature_enabled(self, keys, default=None):
        def nested_get(dictionary, keys_, default_=None):
            if dictionary is None:
                return default_

            if not keys_:
                return dictionary

            if not isinstance(dictionary, dict):
                return default_

            return nested_get(dictionary.get(keys_[0]), keys_[1:], default_)

        return nested_get(self.features, keys, default)


class Connector:
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
        self.source_klass = None
        self.data_provider = None
        self._sync_task = None

    def _update_config(self, doc_source):
        self.status = doc_source["status"]
        self.sync_now = doc_source.get("sync_now", False)
        self.native = doc_source.get("is_native", False)
        self._service_type = doc_source["service_type"]
        self.index_name = doc_source["index_name"]
        self._configuration = DataSourceConfiguration(doc_source["configuration"])
        self.scheduling = doc_source["scheduling"]
        self.pipeline = PipelineSettings(doc_source.get("pipeline", {}))
        self._dirty = True
        self._filtering = Filtering(doc_source.get("filtering", []))
        self.language_code = doc_source["language"]
        self.features = Features(doc_source.get("features", {}))

    @property
    def last_sync_status(self):
        return str2e(self.doc_source.get("last_sync_status"), JobStatus)

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if isinstance(value, str):
            value = str2e(value, Status)
        if not isinstance(value, Status):
            raise TypeError(value)

        self._status = value
        self.doc_source["status"] = e2str(self._status)

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

    @property
    def filtering(self):
        return self._filtering

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

    async def suspend(self):
        if self._sync_task is not None:
            task = self._sync_task
            task.cancel()
            await task
        await self.close()

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

    def start_heartbeat(self, delay):
        if self._heartbeat_started:
            return
        self._heartbeat_started = True

        async def _heartbeat():
            while not self._closed:
                logger.info(f"*** Connector {self.id} HEARTBEAT")
                if not self._syncing:
                    self.doc_source["last_seen"] = iso_utc()
                    await self.sync_doc()
                await asyncio.sleep(delay)

        self._hb = asyncio.create_task(_heartbeat())

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
        job = SyncJob(connector_id=self.id, elastic_index=self.index)
        trigger_method = (
            JobTriggerMethod.ON_DEMAND if self.sync_now else JobTriggerMethod.SCHEDULED
        )
        job_id = await job.start(trigger_method, self.filtering.get_active_filter())

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

    async def _sync_suspended(self, job):
        await job.suspend()
        self.doc_source["last_sync_status"] = e2str(job.status)
        self.doc_source["last_sync_error"] = None
        self.doc_source["error"] = None
        self.doc_source["last_synced"] = iso_utc()
        await self.sync_doc()
        logger.info(f"Sync suspended, Job id: {job.job_id}")

    async def _sync_done(self, job, result, exception=None):
        doc_updated = result.get("doc_updated", 0)
        doc_created = result.get("doc_created", 0)
        doc_deleted = result.get("doc_deleted", 0)
        exception = result.get("fetch_error", exception)

        indexed_count = doc_updated + doc_created

        await job.done(indexed_count, doc_deleted, exception)

        self.doc_source["last_sync_status"] = e2str(job.status)
        if exception is None:
            self.doc_source["last_sync_error"] = None
            self.doc_source["error"] = None
        else:
            self.doc_source["last_sync_error"] = str(exception)
            self.doc_source["error"] = str(exception)
            self.status = Status.ERROR

        self.doc_source["last_synced"] = iso_utc()
        await self.sync_doc()

    async def prepare_docs(self, data_provider, filtering=None):
        if filtering is None:
            filtering = Filter()

        logger.debug(f"Using pipeline {self.pipeline}")

        async for doc, lazy_download in data_provider.get_docs(filtering=filtering):
            # adapt doc for pipeline settings
            doc["_extract_binary_content"] = self.pipeline.extract_binary_content
            doc["_reduce_whitespace"] = self.pipeline.reduce_whitespace
            doc["_run_ml_inference"] = self.pipeline.run_ml_inference
            yield doc, lazy_download

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
                # sets the defaults and the flag to NEEDS_CONFIGURATION
                self.doc_source[
                    "configuration"
                ] = source_klass.get_simple_configuration()
                self.doc_source["status"] = e2str(Status.NEEDS_CONFIGURATION)
                self._update_config(self.doc_source)
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

    async def sync(self, elastic_server, idling):
        # If anything bad happens before we create a sync job
        # (like bad scheduling config, etc.)
        #
        # we will raise the error in the logs here and let Kibana knows
        # by toggling the status and setting the error and status field
        if self.source_klass is None:
            raise Exception("Can't call `sync()` before `prepare()`")

        try:
            service_type = self.service_type
            # if the status is different from comnected
            if self.status != Status.CONNECTED:
                self.status = Status.CONNECTED
                await self.sync_doc()

            next_sync = self.next_sync()
            # First we check if sync is disabled, and it terminates all other conditions
            if next_sync == SYNC_DISABLED:
                logger.debug(f"Scheduling is disabled for {service_type}")
                return
            # Then we check if we need to restart SUSPENDED job
            elif self.last_sync_status == JobStatus.SUSPENDED:
                logger.info("Restarting sync after suspension")
            # And only then we check if we need to run sync right now or not
            elif next_sync - idling > 0:
                logger.debug(
                    f"Next sync for {service_type} due in {int(next_sync)} seconds"
                )
                return

            try:
                self.data_provider = self.source_klass(self.configuration)
            except Exception as e:
                logger.critical(e, exc_info=True)
                raise DataSourceError(
                    f"Could not instantiate {self.source_klass} for {service_type}"
                )

            if not await self.data_provider.changed():
                logger.debug(f"No change in {service_type} data provider, skipping...")
                return
        except Exception as exc:
            self.doc_source["error"] = str(exc)
            self.status = Status.ERROR
            await self.sync_doc()
            raise

        logger.debug(f"Syncing '{service_type}'")
        self._syncing = True
        self._sync_task = asyncio.current_task()
        job = await self._sync_starts()
        try:
            logger.debug(f"Pinging the {self.data_provider} backend")
            await self.data_provider.ping()
            await asyncio.sleep(0)

            mappings = Mappings.default_text_fields_mappings(
                is_connectors_index=True,
            )

            logger.debug("Preparing the content index")
            await elastic_server.prepare_content_index(
                self.index_name, mappings=mappings
            )
            await asyncio.sleep(0)

            # allows the data provider to change the bulk options
            bulk_options = self.bulk_options.copy()
            self.data_provider.tweak_bulk_options(bulk_options)

            sync_rules_enabled = self.features.sync_rules_enabled()

            if sync_rules_enabled:
                await validate_filtering(self, self.index, ValidationTarget.ACTIVE)

            result = await elastic_server.async_bulk(
                self.index_name,
                self.prepare_docs(self.data_provider, job.filtering),
                self.pipeline,
                filter=self.filtering.get_active_filter(),
                sync_rules_enabled=sync_rules_enabled,
                options=bulk_options,
            )
            await self._sync_done(job, result)
        except asyncio.CancelledError:
            await self._sync_suspended(job)
        except Exception as e:
            await self._sync_done(job, {}, exception=e)
            raise
        finally:
            if result is None:
                result = {}
            doc_updated = result.get("doc_updated", 0)
            doc_created = result.get("doc_created", 0)
            doc_deleted = result.get("doc_deleted", 0)
            logger.info(
                f"[{self.id}] Sync done: {doc_updated + doc_created} indexed, {doc_deleted} "
                f" deleted. ({int(time.time() - self._start_time)} seconds)"
            )
            self._syncing = False
            self._start_time = None
            self._sync_task = None


STUCK_JOBS_THRESHOLD = 60  # 60 seconds


class SyncJobIndex(ESIndex):
    """
    Represents Elasticsearch index for sync jobs

    Args:
        elastic_config (dict): Elasticsearch configuration and credentials
    """

    def __init__(self, elastic_config):
        super().__init__(index_name=JOBS_INDEX, elastic_config=elastic_config)

    def _create_object(self, doc_source):
        """
        Args:
            doc_source (dict): A raw Elasticsearch document
        Returns:
            SyncJob
        """
        return SyncJob(
            self,
            connector_id=doc_source["_source"]["connector"]["id"],
            doc_source=doc_source,
        )

    async def pending_jobs(self, connector_ids=[]):
        query = {
            "bool": {
                "must": [
                    {
                        "terms": {
                            "status": [
                                e2str(JobStatus.PENDING),
                                e2str(JobStatus.SUSPENDED),
                            ]
                        }
                    },
                    {"terms": {"connector.id": connector_ids}},
                ]
            }
        }
        async for job in self.get_all_docs(query=query):
            yield job

    async def orphaned_jobs(self, connector_ids=[]):
        query = {"bool": {"must_not": {"terms": {"connector.id": connector_ids}}}}
        async for job in self.get_all_docs(query=query):
            yield job

    async def stuck_jobs(self, connector_ids=[]):
        query = {
            "bool": {
                "filter": [
                    {"terms": {"connector.id": connector_ids}},
                    {
                        "terms": {
                            "status": [
                                e2str(JobStatus.IN_PROGRESS),
                                e2str(JobStatus.CANCELING),
                            ]
                        }
                    },
                    {"range": {"last_seen": {"lte": f"now-{STUCK_JOBS_THRESHOLD}s"}}},
                ]
            }
        }

        async for job in self.get_all_docs(query=query):
            yield job

    async def delete_jobs(self, job_ids=[]):
        query = {"terms": {"_id": job_ids}}
        return await self.client.delete_by_query(index=self.index_name, query=query)
