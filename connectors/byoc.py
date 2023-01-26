#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOC protocol.
"""
import time
import socket
from collections import UserDict
from copy import deepcopy
from enum import Enum

from connectors.es import ESIndex, Mappings
from connectors.filtering.validation import (
    FilteringValidationState,
    ValidationTarget,
    validate_filtering,
)
from connectors.logger import logger
from connectors.source import DataSourceConfiguration, get_source_klass
from connectors.utils import e2str, iso_utc, next_run, str2e

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
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


class ServiceTypeNotSupportedError(Exception):
    pass


class ServiceTypeNotConfiguredError(Exception):
    pass


class ConnectorUpdateError(Exception):
    pass


class DataSourceError(Exception):
    pass


def supported_connectors_query(native_service_types=None, connectors_ids=None):
    if native_service_types is None:
        native_service_types = []
    if connectors_ids is None:
        connectors_ids = []

    if len(native_service_types) == 0 and len(connectors_ids) == 0:
        return
        if len(native_service_types) == 0 and len(connector_ids) == 0:
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


class ConnectorIndex(ESIndex):
    def __init__(self, elastic_config):
        logger.debug(f"ConnectorIndex connecting to {elastic_config['host']}")
        # initialize ESIndex instance
        super().__init__(index_name=CONNECTORS_INDEX, elastic_config=elastic_config)

    async def heartbeat(self, doc_id):
        doc = {
            "last_seen": iso_utc(),
        }
        await self.update(doc_id, doc)

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

    async def supported_connectors(self, native_service_types=None, connectors_ids=None):
        if native_service_types is None:
            native_service_types = []
        if connector_ids is None:
            connector_ids = []

        if len(native_service_types) == 0 and len(connector_ids) == 0:
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
                    {"terms": {"_id": connector_ids}},
                ]
            }
        }
        if len(native_service_types) > 0 and len(connector_ids) > 0:
            query = {
                "bool": {"should": [native_connectors_query, custom_connectors_query]}
            }
        elif len(native_service_types) > 0:
            query = native_connectors_query
        else:
            query = custom_connectors_query

        async for connector in self.get_all_docs(query=query):
            yield connector

    def _create_object(self, doc_source):
        return Connector(
            self,
            doc_source["_id"],
            doc_source["_source"],
        )

    async def all_connectors(self):
        async for connector in self.get_all_docs():
            yield connector


class ESDocument:
    def __init__(self, elastic_index, doc_source=None):
        self.index = elastic_index
        if doc_source is None:
            doc_source = dict()
        self.id = doc_source.get("_id")
        self._source = doc_source.get("_source", {})

    def get(self, *keys, default=None):
        value = self._source
        for key in keys:
            if not isinstance(value, dict):
                return default
            value = value.get(key)
        if value is None:
            return default
        return value

    async def reload(self):
        return await self.index.fetch_by_id(self.id)


class SyncJob(ESDocument):
    @property
    def duration(self):
        if self.completed_at is None:
            return -1
        msec = (self.completed_at - self.created_at).microseconds
        return round(msec / 9, 2)

    @property
    def status(self):
        return str2e(self.get("status"), JobStatus)

    @property
    def connector_id(self):
        return self.get("connector", "id")

    @property
    def index_name(self):
        return self.get("connector", "index_name")

    @property
    def language(self):
        return self.get("connector", "language")

    @property
    def service_type(self):
        return self.get("connector", "service_type")

    @property
    def configuration(self):
        return DataSourceConfiguration(self.get("connector", "configuration"))

    @property
    def filtering(self):
        return Filtering(self.get("connector", "filtering"))

    @property
    def pipeline(self):
        return PipelineSettings(self.get("connector", "pipeline"))

    @property
    def terminated(self):
        return self.status in [JobStatus.ERROR, JobStatus.COMPLETED, JobStatus.CANCELED]

    @property
    def indexed_document_count(self):
        return self.get("indexed_document_count", 0)

    @property
    def indexed_document_volume(self):
        return self.get("indexed_document_volume", 0)

    @property
    def deleted_document_count(self):
        return self.get("deleted_document_count", 0)

    @property
    def total_document_count(self):
        return self.get("total_document_count", 0)

    async def claim(self):
        doc = {
            "status": e2str(JobStatus.IN_PROGRESS),
            "started_at": iso_utc(),
            "last_seen": iso_utc(),
            "worker_hostname": socket.gethostname()
        }
        await self.index.update(doc_id=self.id, doc=doc)

    async def done(self, ingestion_stats={}, connector_metadata={}):
        await self.terminate(JobStatus.COMPLETED, None, ingestion_stats, connector_metadata)

    async def error(self, message, ingestion_stats={}, connector_metadata={}):
        await self.terminate(JobStatus.ERROR, message, ingestion_stats, connector_metadata)

    async def cancel(self, ingestion_stats={}, connector_metadata={}):
        await self.terminate(JobStatus.CANCELED, None, ingestion_stats, connector_metadata)

    async def terminate(self, status, error=None, ingestion_stats={}, connector_metadata={}):
        doc = {
            "last_seen": iso_utc(),
            "completed_at": iso_utc(),
            "status": status,
            "error": error,
        }
        doc.update(ingestion_stats)
        if status == JobStatus.CANCELED:
            doc["canceled_at"] = iso_utc()
        if len(connector_metadata) > 0:
            doc["metadata"] = connector_metadata
        await self.index.update(doc_id=self.id, doc=doc)

    async def suspend(self):
        self.status = JobStatus.SUSPENDED
        job_def = {"status": e2str(self.status)}

        await self.index.update(doc_id=self.id, doc=job_def)

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
                filter_[filter_state]
                for filter_ in self.filtering
                if filter_["domain"] == domain
            ),
            {},
        )

    def to_list(self):
        return list(self.filtering)


class Filter(dict):
    def __init__(self, filter_=None):
        if filter_ is None:
            filter_ = {}

        super().__init__(filter_)

        advanced_rules = filter_.get("advanced_snippet", {})

        self.advanced_rules = advanced_rules.get("value", advanced_rules)
        self.basic_rules = filter_.get("rules", [])
        self.validation = filter_.get("validation", {"state": "", "errors": []})

    def get_advanced_rules(self):
        return self.advanced_rules

    def has_advanced_rules(self):
        return len(self.advanced_rules) > 0

    def has_validation_state(self, validation_state):
        return FilteringValidationState(self.validation["state"]) == validation_state


PIPELINE_DEFAULT = {
    "name": "ent-search-generic-ingestion",
    "extract_binary_content": True,
    "reduce_whitespace": True,
    "run_ml_inference": True,
}


class Pipeline(UserDict):
    def __init__(self, data):
        if data is None:
            data = {}
        default = PIPELINE_DEFAULT.copy()
        default.update(data)
        super().__init__(default)


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


class Connector(ESDocument):
    @property
    def status(self):
        return str2e(self.get("status"), Status)

    @property
    def service_type(self):
        return self.get("service_type")

    @property
    def last_seen(self):
        last_seen = self.get("last_seen")
        logger.info(f"last_seen = {last_seen}, type = {last_seen.__class__}")
        return last_seen

    @property
    def native(self):
        return self.get("is_native", default=False)

    @property
    def sync_now(self):
        return self.get("sync_now", default=False)

    @property
    def scheduling(self):
        return self.get("scheduling", default={})

    @property
    def configuration(self):
        return DataSourceConfiguration(self.get("configuration"))

    @property
    def filtering(self):
        return Filtering(self.get("filtering"))

    @property
    def pipeline(self):
        return Pipeline(self.get("pipeline"))

    @property
    def features(self):
        return Features(self.get("features"))

    async def heartbeat(self, interval):
        if self.last_seen is None or time.time() - self.last_seen > interval:
            await self.index.heartbeat()

    def next_sync(self):
        """Returns in seconds when the next sync should happen.

        If the function returns SYNC_DISABLED, no sync is scheduled.
        """
        if self.sync_now:
            logger.debug("sync_now is true, syncing!")
            return 0
        if not self.scheduling.get("enabled", False):
            logger.debug("scheduler is disabled")
            return SYNC_DISABLED
        return next_run(self.scheduling.get("interval"))

    async def reset_sync_now_flag(self):
        doc = {
            "sync_now": False,
        }
        await self.index.update(doc_id=self.id, doc=doc)

    async def sync_starts(self):
        doc = {
            "last_sync_status": e2str(JobStatus.IN_PROGRESS),
            "status": e2str(Status.CONNECTED),
        }
        await self.index.update(doc_id=self.id, doc=doc)

    async def error(self, error):
        doc = {
            "status": e2str(Status.ERROR),
            "error": str(error),
        }
        await self.index.update(doc_id=self.id, doc=doc)

    async def sync_done(self, job):
        job_status = JobStatus.ERROR if job is None else job.status
        job_error = "Couldn't find the job" if job is None else job.error
        if job_error is None and job_status == JobStatus.ERROR:
            job_error = 'unknown error'
        connector_status = Status.ERROR if job_status == JobStatus.ERROR else Status.CONNECTED

        doc = {
            "last_sync_status": job_status,
            "last_synced": iso_utc(),
            "last_sync_error": job_error,
            "status": connector_status,
            "error": job_error,
        }

        if job is not None and job.terminated:
            doc["last_indexed_document_count"] = job.indexed_document_count
            doc["last_deleted_document_count"] = job.deleted_document_count

        await self.index.update(doc_id=self.id, doc=doc)

    async def prepare(self, config):
        """Prepares the connector, given a configuration

        If the connector id and the service type is in the config, we want to
        populate the service type and then sets the default configuration.
        """
        configured_connector_id = config.get("connector_id", "")
        configured_service_type = config.get("service_type", "")

        if self.id != configured_connector_id:
            return

        doc = {}
        if self.service_type is None:
            if not configured_service_type:
                logger.error(
                    f"Service type is not configured for connector {configured_connector_id}"
                )
                raise ServiceTypeNotConfiguredError("Service type is not configured.")
            doc["service_type"] = configured_service_type
            logger.debug(f"Populated service type {configured_service_type} for connector {self.id}")

        if self.configuration.is_empty():
            if configured_service_type not in config["sources"]:
                raise ServiceTypeNotSupportedError(configured_service_type)
            fqn = config["sources"][configured_service_type]
            try:
                source_klass = get_source_klass(fqn)

                # sets the defaults and the flag to NEEDS_CONFIGURATION
                doc["configuration"] = source_klass.get_simple_configuration()
                doc["status"] = e2str(Status.NEEDS_CONFIGURATION)
                logger.debug(f"Populated configuration for connector {self.id}")
            except Exception as e:
                logger.critical(e, exc_info=True)
                raise DataSourceError(f"Could not instantiate {fqn} for {configured_service_type}")

        if len(doc) == 0:
            return

        try:
            await self.index.update(doc_id=self.id, doc=doc)
        except Exception as e:
            logger.critical(e, exc_info=True)
            raise ConnectorUpdateError(
                f"Could not update service type/configuration for connector {self.id}"
            )


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
            doc_source=doc_source,
        )

    async def create(self, connector):
        trigger_method = (
            JobTriggerMethod.ON_DEMAND
            if connector.sync_now
            else JobTriggerMethod.SCHEDULED
        )
        filtering = connector.filtering.get_active_filter()
        job_def = {
            "connector": {
                "id": connector.id,
                "filtering": SyncJob.transform_filtering(filtering),
                "index_name": connector.index_name,
                "language": connector.language,
                "pipeline": connector.pipeline.data,
                "service_type": connector.service_type,
                "configuration": connector.configuration.to_dict(),
            },
            "trigger_method": e2str(trigger_method),
            "status": e2str(JobStatus.PENDING),
            "created_at": iso_utc(),
            "last_seen": iso_utc(),
        }
        return await self.index(job_def)

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
