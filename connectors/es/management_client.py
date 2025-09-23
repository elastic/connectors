#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import partial

from elasticsearch import ApiError
from elasticsearch import (
    NotFoundError as ElasticNotFoundError,
)
from elasticsearch.helpers import async_scan

from connectors.es import TIMESTAMP_FIELD
from connectors.es.client import ESClient
from connectors.logger import logger


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

    def __init__(self, config) -> None:
        logger.debug(f"ESManagementClient connecting to {config['host']}")
        # initialize ESIndex instance
        super().__init__(config)

    async def ensure_exists(self, indices=None) -> None:
        if indices is None:
            indices = []

        for index in indices:
            logger.debug(f"Checking index {index}")
            if not await self._retrier.execute_with_retry(
                partial(self.client.indices.exists, index=index)
            ):
                await self._retrier.execute_with_retry(
                    partial(self.client.indices.create, index=index)
                )
                logger.debug(f"Created index {index}")

    async def create_content_index(self, search_index_name, language_code):
        return await self._retrier.execute_with_retry(
            partial(
                self.client.indices.create,
                index=search_index_name,
            )
        )

    async def ensure_ingest_pipeline_exists(
        self, pipeline_id, version, description, processors
    ) -> None:
        try:
            await self._retrier.execute_with_retry(
                partial(self.client.ingest.get_pipeline, id=pipeline_id)
            )
        except ElasticNotFoundError:
            await self._retrier.execute_with_retry(
                partial(
                    self.client.ingest.put_pipeline,
                    id=pipeline_id,
                    version=version,
                    description=description,
                    processors=processors,
                )
            )

    async def delete_indices(self, indices) -> None:
        await self._retrier.execute_with_retry(
            partial(self.client.indices.delete, index=indices, ignore_unavailable=True)
        )

    async def clean_index(self, index_name):
        return await self._retrier.execute_with_retry(
            partial(
                self.client.delete_by_query,
                index=index_name,
                body={"query": {"match_all": {}}},
                ignore_unavailable=True,
            )
        )

    async def list_indices(self, index: str = "*"):
        """
        List indices using Elasticsearch.stats API. Includes the number of documents in each index.
        """
        indices = {}
        response = await self._retrier.execute_with_retry(
            partial(self.client.indices.stats, index=index)
        )

        for index in response["indices"].items():
            indices[index[0]] = {"docs_count": index[1]["primaries"]["docs"]["count"]}

        return indices

    async def list_indices_serverless(self, index: str = "*"):
        """
        List indices in a serverless environment. This method is a workaround to the fact that
        the `indices.stats` API is not available in serverless environments.
        """

        indices = {}
        try:
            response = await self._retrier.execute_with_retry(
                partial(self.client.indices.get, index=index)
            )

            for index in response.items():
                indices[index[0]] = {}

        except ApiError as e:
            logger.error(f"Error listing indices: {e}")

        return indices

    async def index_exists(self, index_name):
        return await self._retrier.execute_with_retry(
            partial(self.client.indices.exists, index=index_name)
        )

    async def get_index_or_alias(self, index_name, ignore_unavailable: bool = False):
        """
        Get index definition (mappings and settings) by its name or its alias.
        """
        # Example structure of response:
        # {
        #     "my-data-index": {
        #         "aliases": {"search-my-data-index": {}},
        #         "mappings": { ... },
        #         "settings": { ... },
        #     }
        # }
        get_index_response = await self._retrier.execute_with_retry(
            partial(
                self.client.indices.get,
                index=index_name,
                ignore_unavailable=ignore_unavailable,
            )
        )

        if index_name in get_index_response:
            logger.debug(f"Got index by its name: {index_name}")
            return get_index_response[index_name]

        for (
            existing_index_name,
            existing_index_definition,
        ) in get_index_response.items():
            if "aliases" not in existing_index_definition:
                continue

            if index_name in existing_index_definition["aliases"]:
                logger.debug(
                    f"Got index {existing_index_name} by its alias {index_name}"
                )
                return existing_index_definition

        return None

    async def upsert(self, _id, index_name, doc):
        return await self._retrier.execute_with_retry(
            partial(
                self.client.index,
                id=_id,
                index=index_name,
                document=doc,
            )
        )

    async def bulk_insert(self, operations, pipeline):
        return await self._retrier.execute_with_retry(
            partial(
                self.client.bulk,
                operations=operations,
                pipeline=pipeline,
            )
        )

    async def yield_existing_documents_metadata(self, index):
        """Returns an iterator on the `id` and `_timestamp` fields of all documents in an index.

        WARNING

        This function will load all ids in memory -- on very large indices,
        depending on the id length, it can be quite large.

        300,000 ids will be around 50MiB
        """
        logger.debug(f"Scanning existing index {index}")
        if not await self.index_exists(index):
            return

        async for doc in async_scan(
            client=self.client, index=index, _source=["id", TIMESTAMP_FIELD]
        ):
            source = doc["_source"]
            doc_id = source.get("id", doc["_id"])
            timestamp = source.get(TIMESTAMP_FIELD)

            yield doc_id, timestamp

    async def get_connector_secret(self, connector_secret_id):
        secret = await self._retrier.execute_with_retry(
            partial(
                self.client.perform_request,
                "GET",
                f"/_connector/_secret/{connector_secret_id}",
            )
        )
        return secret.get("value")

    async def create_connector_secret(self, secret_value):
        secret = await self._retrier.execute_with_retry(
            partial(
                self.client.perform_request,
                "POST",
                "/_connector/_secret",
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                },
                body={"value": secret_value},
            )
        )
        return secret.get("id")
