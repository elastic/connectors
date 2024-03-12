#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from unittest import mock
from unittest.mock import ANY, AsyncMock, Mock

import pytest
import pytest_asyncio
from elasticsearch import (
    NotFoundError as ElasticNotFoundError,
)

from connectors.es.management_client import ESManagementClient
from tests.commons import AsyncIterator


class TestESManagementClient:
    @pytest_asyncio.fixture
    def es_management_client(self):
        config = {
            "username": "elastic",
            "password": "changeme",
            "host": "http://nowhere.com:9200",
        }
        es_management_client = ESManagementClient(config)

        es_management_client.client = AsyncMock()

        yield es_management_client

    @pytest.mark.asyncio
    async def test_ensure_exists_when_no_indices_passed(self, es_management_client):
        await es_management_client.ensure_exists()

        es_management_client.client.indices.exists.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_exists_when_indices_passed(self, es_management_client):
        index_name = "search-mongo"
        es_management_client.client.indices.exists.return_value = False

        await es_management_client.ensure_exists([index_name])

        es_management_client.client.indices.exists.assert_called_with(index=index_name)
        es_management_client.client.indices.create.assert_called_with(index=index_name)

    @pytest.mark.asyncio
    async def test_create_content_index(self, es_management_client):
        index_name = "search-mongo"
        lang_code = "en"
        await es_management_client.create_content_index(index_name, lang_code)

        es_management_client.client.indices.create.assert_called_with(
            index=index_name, mappings=ANY, settings=ANY
        )

    @pytest.mark.asyncio
    async def test_ensure_content_index_mappings_when_mappings_exist(
        self, es_management_client
    ):
        index_name = "something"
        mappings = {}
        existing_mappings_response = {index_name: {"mappings": ["something"]}}

        es_management_client.client.indices.get_mapping = AsyncMock(
            return_value=existing_mappings_response
        )

        await es_management_client.ensure_content_index_mappings(index_name, mappings)
        es_management_client.client.indices.put_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_content_index_mappings_when_mappings_do_not_exist(
        self, es_management_client
    ):
        index_name = "something"
        mappings = {
            "dynamic": True,
            "dynamic_templates": ["something"],
            "properties": "something_else",
        }
        existing_mappings_response = {index_name: {"mappings": {}}}

        es_management_client.client.indices.get_mapping = AsyncMock(
            return_value=existing_mappings_response
        )

        await es_management_client.ensure_content_index_mappings(index_name, mappings)
        es_management_client.client.indices.put_mapping.assert_awaited_with(
            index=index_name,
            dynamic=mappings["dynamic"],
            dynamic_templates=mappings["dynamic_templates"],
            properties=mappings["properties"],
        )

    @pytest.mark.asyncio
    async def test_ensure_content_index_mappings_when_mappings_do_not_exist_but_no_mappings_provided(
        self, es_management_client
    ):
        index_name = "something"
        mappings = None
        existing_mappings_response = {index_name: {"mappings": {}}}

        es_management_client.client.indices.get_mapping = AsyncMock(
            return_value=existing_mappings_response
        )

        await es_management_client.ensure_content_index_mappings(index_name, mappings)
        es_management_client.client.indices.put_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_ingest_pipeline_exists_when_pipeline_do_not_exist(
        self, es_management_client
    ):
        pipeline_id = 1
        version = 2
        description = "that's a pipeline"
        processors = ["something"]

        error_meta = Mock()
        error_meta.status = 404
        error = ElasticNotFoundError("1", error_meta, "3")

        es_management_client.client.ingest.get_pipeline.side_effect = error

        await es_management_client.ensure_ingest_pipeline_exists(
            pipeline_id, version, description, processors
        )

        es_management_client.client.ingest.put_pipeline.assert_awaited_with(
            id=pipeline_id,
            version=version,
            description=description,
            processors=processors,
        )

    @pytest.mark.asyncio
    async def test_ensure_ingest_pipeline_exists_when_pipeline_exists(
        self, es_management_client
    ):
        pipeline_id = 1
        version = 2
        description = "that's a pipeline"
        processors = ["something"]

        es_management_client.client.ingest.get_pipeline = AsyncMock()

        await es_management_client.ensure_ingest_pipeline_exists(
            pipeline_id, version, description, processors
        )

        es_management_client.client.ingest.put_pipeline.assert_not_called()

    @pytest.mark.asyncio
    async def test_delete_indices(self, es_management_client):
        indices = ["search-mongo"]
        es_management_client.client.indices.delete = AsyncMock()

        await es_management_client.delete_indices(indices=indices)
        es_management_client.client.indices.delete.assert_awaited_with(
            index=indices, ignore_unavailable=True
        )

    @pytest.mark.asyncio
    async def test_index_exists(self, es_management_client):
        index_name = "search-mongo"
        es_management_client.client.indices.exists = AsyncMock()

        await es_management_client.index_exists(index_name=index_name)
        es_management_client.client.indices.exists.assert_awaited_with(index=index_name)

    @pytest.mark.asyncio
    async def test_clean_index(self, es_management_client):
        index_name = "search-mongo"
        es_management_client.client.indices.exists = AsyncMock()

        await es_management_client.clean_index(index_name=index_name)
        es_management_client.client.delete_by_query.assert_awaited_with(
            index=index_name, body=ANY, ignore_unavailable=ANY
        )

    @pytest.mark.asyncio
    async def test_list_indices(self, es_management_client):
        await es_management_client.list_indices(index="search-*")

        es_management_client.client.indices.stats.assert_awaited_with(index="search-*")

    @pytest.mark.asyncio
    async def test_index_exists(self, es_management_client):
        index_name = "search-mongo"
        es_management_client.client.indices.exists = AsyncMock(return_value=True)

        assert await es_management_client.index_exists(index_name=index_name) is True

    @pytest.mark.asyncio
    async def test_upsert(self, es_management_client):
        _id = "123"
        index_name = "search-mongo"
        document = {"something": "something"}

        await es_management_client.upsert(_id, index_name, document)

        assert await es_management_client.client.index(
            id=_id, index=index_name, doc=document
        )

    @pytest.mark.asyncio
    async def test_yield_existing_documents_metadata_when_index_does_not_exist(
        self, es_management_client, mock_responses
    ):
        es_management_client.index_exists = AsyncMock(return_value=False)

        records = [
            {"_id": "1", "_source": {"_timestamp": str(datetime.now())}},
            {"_id": "2", "_source": {"_timestamp": str(datetime.now())}},
        ]

        with mock.patch(
            "connectors.es.management_client.async_scan",
            return_value=AsyncIterator(records),
        ):
            ids = []
            async for doc_id, _ in es_management_client.yield_existing_documents_metadata(
                "something"
            ):
                ids.append(doc_id)

            assert ids == []

    @pytest.mark.asyncio
    async def test_yield_existing_documents_metadata_when_index_exists(
        self, es_management_client, mock_responses
    ):
        es_management_client.index_exists = AsyncMock(return_value=True)

        records = [
            {"_id": "1", "_source": {"_timestamp": str(datetime.now())}},
            {"_id": "2", "_source": {"_timestamp": str(datetime.now())}},
        ]

        with mock.patch(
            "connectors.es.management_client.async_scan",
            return_value=AsyncIterator(records),
        ):
            ids = []
            async for doc_id, _ in es_management_client.yield_existing_documents_metadata(
                "something"
            ):
                ids.append(doc_id)

            assert ids == ["1", "2"]

    @pytest.mark.asyncio
    async def test_get_connector_secret(self, es_management_client, mock_responses):
        secret_id = "secret-id"

        es_management_client.client.perform_request = AsyncMock(
            return_value={"id": secret_id, "value": "secret-value"}
        )

        secret = await es_management_client.get_connector_secret(secret_id)
        assert secret == "secret-value"
        es_management_client.client.perform_request.assert_awaited_with(
            "GET", f"/_connector/_secret/{secret_id}"
        )

    @pytest.mark.asyncio
    async def test_get_connector_secret_when_secret_does_not_exist(
        self, es_management_client, mock_responses
    ):
        secret_id = "secret-id"

        error_meta = Mock()
        error_meta.status = 404
        es_management_client.client.perform_request = AsyncMock(
            side_effect=ElasticNotFoundError(
                "resource_not_found_exception",
                error_meta,
                f"No secret with id [{secret_id}]",
            )
        )

        with pytest.raises(ElasticNotFoundError):
            secret = await es_management_client.get_connector_secret(secret_id)
            assert secret is None
