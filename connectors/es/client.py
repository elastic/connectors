#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import functools
import logging
import os
import time
from enum import Enum

from elastic_transport.client_utils import url_to_node_config
from elasticsearch import ApiError, AsyncElasticsearch, ConflictError
from elasticsearch import (
    ConnectionError as ElasticConnectionError,
)
from elasticsearch import (
    NotFoundError as ElasticNotFoundError,
)
from elasticsearch.helpers import async_scan

from connectors import __version__
from connectors.es.settings import TIMESTAMP_FIELD, Mappings, Settings
from connectors.logger import logger, set_extra_logger
from connectors.utils import CancellableSleeps


class License(Enum):
    ENTERPRISE = "enterprise"
    PLATINUM = "platinum"
    GOLD = "gold"
    BASIC = "basic"
    TRIAL = "trial"
    EXPIRED = "expired"
    UNSET = None


class ESClient:
    def __init__(self, config):
        # We don't have a way to ask the server, but it's planned
        # for now we just use an env flag
        self.serverless = "SERVERLESS" in os.environ
        self.config = config
        self.host = url_to_node_config(
            config.get("host", "http://localhost:9200"),
            use_default_ports_for_scheme=True,
        )
        self._sleeps = CancellableSleeps()
        options = {
            "hosts": [self.host],
            "request_timeout": config.get("request_timeout", 120),
            "retry_on_timeout": config.get("retry_on_timeout", True),
        }
        logger.debug(f"Host is {self.host}")

        if "api_key" in config:
            logger.debug(f"Connecting with an API Key ({config['api_key'][:5]}...)")
            options["api_key"] = config["api_key"]
            if "username" in config or "password" in config:
                msg = "configured API key will be used over configured basic auth"
                if (
                    config.get("username") == "elastic"
                    and config.get("password") == "changeme"
                ):
                    logger.debug(
                        msg
                    )  # don't cause a panic if it's just the default creds
                else:
                    logger.warning(msg)
        elif "username" in config:
            auth = config["username"], config["password"]
            options["basic_auth"] = auth
            logger.debug(f"Connecting using Basic Auth (user: {config['username']})")

        if config.get("ssl", False):
            options["verify_certs"] = True
            if "ca_certs" in config:
                ca_certs = config["ca_certs"]
                logger.debug(f"Verifying cert with {ca_certs}")
                options["ca_certs"] = ca_certs

        level = config.get("log_level", "INFO").upper()
        es_logger = logging.getLogger("elastic_transport.node")
        set_extra_logger(
            es_logger,
            log_level=logging.getLevelName(level),
            filebeat=logger.filebeat,  # pyright: ignore
        )
        self.max_wait_duration = config.get("max_wait_duration", 60)
        self.initial_backoff_duration = config.get("initial_backoff_duration", 5)
        self.backoff_multiplier = config.get("backoff_multiplier", 2)
        options["headers"] = config.get("headers", {})
        options["headers"]["user-agent"] = f"elastic-connectors-{__version__}"
        self.client = AsyncElasticsearch(**options)
        self._keep_waiting = True

    def stop_waiting(self):
        self._keep_waiting = False
        self._sleeps.cancel()

    async def has_active_license_enabled(self, license_):
        """This method checks, whether an active license or a more powerful active license is enabled.

        Returns:
            Tuple: (boolean if `license_` is enabled and not expired, actual license Elasticsearch is using)
        """

        license_response = await self.client.license.get()
        license_info = license_response.get("license", {})
        is_expired = license_info.get("status", "").lower() == "expired"

        if is_expired:
            return False, License.EXPIRED

        actual_license = License(license_info.get("type").lower())

        license_order = [
            License.BASIC,
            License.GOLD,
            License.PLATINUM,
            License.ENTERPRISE,
            License.TRIAL,
        ]

        license_index = license_order.index(actual_license)

        return (
            license_order.index(license_) <= license_index,
            actual_license,
        )

    async def close(self):
        await self.client.close()

    async def ping(self):
        try:
            await self.client.info()
        except ApiError as e:
            logger.error(f"The server returned a {e.status_code} code")
            if e.info is not None and "error" in e.info and "reason" in e.info["error"]:
                logger.error(e.info["error"]["reason"])
            return False
        except ElasticConnectionError as e:
            logger.error("Could not connect to the server")
            if e.message is not None:
                logger.error(e.message)
            return False
        return True

    async def wait(self):
        backoff = self.initial_backoff_duration
        start = time.time()
        logger.debug(f"Wait for Elasticsearch (max: {self.max_wait_duration})")
        while time.time() - start < self.max_wait_duration:
            if not self._keep_waiting:
                await self.close()
                return False

            logger.info(
                f"Waiting for {self.host} (so far: {int(time.time() - start)} secs)"
            )
            if await self.ping():
                return True
            await self._sleeps.sleep(backoff)
            backoff *= self.backoff_multiplier

        await self.close()
        return False


class ESManagementClient(ESClient):
    """
    Elasticsearch client with methods to manage connector-related indices.

    Additionally to regular methods of ESClient, this client provides methods to work with arbitrary indices,
    for example allowing to list indices, delete indices, wipe data from indices and such.

    ESClient should be used to provide rich clients that operate on "domains", such as:
        - specific connector
        - specific job

    This client, on the contrary, is used to manage a number of indices outside of connector protocol operations.
    """

    def __init__(self, config):
        logger.debug(f"ESManagementClient connecting to {config['host']}")
        # initialize ESIndex instance
        super().__init__(config)

    async def ensure_exists(self, indices=None, expand_wildcards="open"):
        if indices is None:
            indices = []

        for index in indices:
            logger.debug(
                f"Checking index {index} with expand_wildcards={expand_wildcards}"
            )
            if not await self.client.indices.exists(
                index=index, expand_wildcards=expand_wildcards
            ):
                await self.client.indices.create(index=index)
                logger.debug(f"Created index {index}")

    async def create_content_index(self, search_index_name, language_code):
        settings = Settings(language_code=language_code, analysis_icu=False).to_hash()
        mappings = Mappings.default_text_fields_mappings(is_connectors_index=True)

        return await self.client.indices.create(
            index=search_index_name, mappings=mappings, settings=settings
        )

    async def ensure_content_index_mappings(self, index, mappings):
        # open = Match open, non-hidden indices. Also matches any non-hidden data stream.
        # Content indices are always non-hidden.
        expand_wildcards = "open"
        response = await self.client.indices.get_mapping(
            index=index, expand_wildcards=expand_wildcards
        )

        existing_mappings = response[index].get("mappings", {})
        if len(existing_mappings) == 0 and mappings:
            logger.debug(
                "Index %s has no mappings or it's empty. Adding mappings...", index
            )
            await self.client.indices.put_mapping(
                index=index,
                dynamic=mappings.get("dynamic", False),
                dynamic_templates=mappings.get("dynamic_templates", []),
                properties=mappings.get("properties", {}),
                expand_wildcards=expand_wildcards,
            )
            logger.debug("Successfully added mappings for index %s", index)
        else:
            logger.debug(
                "Index %s already has mappings, skipping mappings creation", index
            )

    async def ensure_ingest_pipeline_exists(
        self, pipeline_id, version, description, processors
    ):
        try:
            await self.client.ingest.get_pipeline(id=pipeline_id)
        except ElasticNotFoundError:
            await self.client.ingest.put_pipeline(
                id=pipeline_id,
                version=version,
                description=version,
                processors=version,
            )

    async def delete_indices(self, indices, expand_wildcards="open"):
        await self.client.indices.delete(
            index=indices, expand_wildcards=expand_wildcards, ignore_unavailable=True
        )

    async def clean_index(self, index_name):
        return await self.client.delete_by_query(
            index=index_name, body={"query": {"match_all": {}}}, ignore_unavailable=True
        )

    async def list_indices(self):
        return await self.client.indices.stats(index="search-*")

    async def index_exists(self, index_name, expand_wildcards="open"):
        return await self.client.indices.exists(
            index=index_name, expand_wildcards=expand_wildcards
        )

    async def upsert(self, _id, index_name, doc):
        await self.client.index(
            id=_id,
            index=index_name,
            document=doc,
        )

    async def yield_existing_documents_metadata(self, index):
        """Returns an iterator on the `id` and `_timestamp` fields of all documents in an index.

        WARNING

        This function will load all ids in memory -- on very large indices,
        depending on the id length, it can be quite large.

        300,000 ids will be around 50MiB
        """
        logger.debug(f"Scanning existing index {index}")
        if not self.index_exists(index):
            return

        async for doc in async_scan(
            client=self.client, index=index, _source=["id", TIMESTAMP_FIELD]
        ):
            source = doc["_source"]
            doc_id = source.get("id", doc["_id"])
            timestamp = source.get(TIMESTAMP_FIELD)

            yield doc_id, timestamp


def with_concurrency_control(retries=3):
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            retry = 1
            while retry <= retries:
                try:
                    return await func(*args, **kwargs)
                except ConflictError as e:
                    logger.debug(
                        f"A conflict error was returned from elasticsearch: {e.message}"
                    )
                    if retry >= retries:
                        raise e
                    retry += 1

        return wrapped

    return wrapper
