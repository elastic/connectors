#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from unittest.mock import AsyncMock

import pytest

from connectors.sources.elasticsearch_mappings import ElasticsearchMappingsDataSource
from tests.sources.support import assert_basics, create_source


@pytest.mark.asyncio
async def test_basics():
    await assert_basics(
        ElasticsearchMappingsDataSource, "host", "http://localhost:9200"
    )


@pytest.mark.asyncio
async def test_ping_success():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        source.es_client.ping = AsyncMock(return_value={"name": "test"})
        result = await source.ping()
        assert result is True


@pytest.mark.asyncio
async def test_ping_failure():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        source.es_client.ping = AsyncMock(return_value=None)
        result = await source.ping()
        assert result is False


@pytest.mark.asyncio
async def test_get_docs():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        # Mock ES client methods
        source.es_client.client.cat.indices = AsyncMock(
            return_value=[
                {"index": "test-index-1"},
                {"index": "test-index-2"},
                {"index": ".system-index"},  # should be filtered out by default
            ]
        )

        source.es_client.client.indices.get_mapping = AsyncMock(
            side_effect=[
                {
                    "test-index-1": {
                        "mappings": {
                            "properties": {
                                "field1": {"type": "text"},
                                "field2": {"type": "keyword"},
                                "nested_field": {
                                    "type": "nested",
                                    "properties": {"inner_field": {"type": "text"}},
                                },
                            }
                        }
                    }
                },
                {
                    "test-index-2": {
                        "mappings": {"properties": {"simple_field": {"type": "text"}}}
                    }
                },
            ]
        )

        documents = []
        async for doc, _lazy_download in source.get_docs():
            documents.append(doc)
            assert _lazy_download is None  # No lazy download for this connector

        assert len(documents) == 2

        # Check first document
        doc1 = documents[0]
        assert doc1["_id"] == "test-index-1"
        assert doc1["index_name"] == "test-index-1"
        assert "mapping" in doc1
        # mapping should be a JSON string, not an object
        import json

        mapping_obj = json.loads(doc1["mapping"])
        assert "properties" in mapping_obj
        assert doc1["source_cluster"] == "http://localhost:9200"

        # Check second document
        doc2 = documents[1]
        assert doc2["_id"] == "test-index-2"
        assert doc2["index_name"] == "test-index-2"


@pytest.mark.asyncio
async def test_get_docs_with_system_indices():
    async with create_source(
        ElasticsearchMappingsDataSource, include_system_indices=True
    ) as source:
        # Mock ES client methods
        source.es_client.client.cat.indices = AsyncMock(
            return_value=[{"index": "test-index"}, {"index": ".kibana"}]
        )

        source.es_client.client.indices.get_mapping = AsyncMock(
            side_effect=[
                {
                    "test-index": {
                        "mappings": {"properties": {"field1": {"type": "text"}}}
                    }
                },
                {
                    ".kibana": {
                        "mappings": {"properties": {"config": {"type": "object"}}}
                    }
                },
            ]
        )

        documents = []
        async for doc, _lazy_download in source.get_docs():
            documents.append(doc)

        assert len(documents) == 2
        index_names = {doc["index_name"] for doc in documents}
        assert "test-index" in index_names
        assert ".kibana" in index_names


@pytest.mark.asyncio
async def test_get_docs_with_auth():
    async with create_source(
        ElasticsearchMappingsDataSource, username="elastic", password="changeme"
    ) as source:
        # Mock ES client methods
        source.es_client.client.cat.indices = AsyncMock(
            return_value=[{"index": "test-index"}]
        )

        source.es_client.client.indices.get_mapping = AsyncMock(
            return_value={
                "test-index": {"mappings": {"properties": {"field1": {"type": "text"}}}}
            }
        )

        documents = []
        async for doc, _lazy_download in source.get_docs():
            documents.append(doc)

        assert len(documents) == 1
        assert documents[0]["index_name"] == "test-index"


@pytest.mark.asyncio
async def test_get_docs_with_meta_and_field_descriptions():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        # Mock ES client methods
        source.es_client.client.cat.indices = AsyncMock(
            return_value=[{"index": "test-index-with-meta"}]
        )

        source.es_client.client.indices.get_mapping = AsyncMock(
            return_value={
                "test-index-with-meta": {
                    "mappings": {
                        "_meta": {
                            "version": "1.0",
                            "author": "test-user",
                            "description": "Test index with metadata",
                        },
                        "properties": {
                            "field1": {
                                "type": "text",
                                "meta": {"description": "First field description"},
                            },
                            "field2": {
                                "type": "keyword",
                                "meta": {"description": "Second field description"},
                            },
                            "nested_field": {
                                "type": "nested",
                                "properties": {
                                    "inner_field": {
                                        "type": "text",
                                        "meta": {
                                            "description": "Nested field description"
                                        },
                                    }
                                },
                            },
                        },
                    }
                }
            }
        )

        documents = []
        async for doc, _lazy_download in source.get_docs():
            documents.append(doc)

        assert len(documents) == 1
        doc = documents[0]

        # Check _meta properties are extracted
        assert doc["meta_version"] == "1.0"
        assert doc["meta_author"] == "test-user"
        assert doc["meta_description"] == "Test index with metadata"

        # Check field descriptions are extracted
        assert "field_descriptions" in doc
        field_descriptions = doc["field_descriptions"]
        assert len(field_descriptions) == 3
        assert "First field description" in field_descriptions
        assert "Second field description" in field_descriptions
        assert "Nested field description" in field_descriptions


@pytest.mark.asyncio
async def test_get_docs_empty_mapping():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        # Mock ES client methods
        source.es_client.client.cat.indices = AsyncMock(
            return_value=[{"index": "empty-index"}]
        )

        source.es_client.client.indices.get_mapping = AsyncMock(
            return_value={"empty-index": {"mappings": {}}}
        )

        documents = []
        async for doc, _lazy_download in source.get_docs():
            documents.append(doc)

        assert len(documents) == 1
        doc = documents[0]
        assert doc["_id"] == "empty-index"
        assert doc["index_name"] == "empty-index"


@pytest.mark.asyncio
async def test_changed():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        result = await source.changed()
        assert result is True


@pytest.mark.asyncio
async def test_close():
    async with create_source(ElasticsearchMappingsDataSource) as source:
        source.es_client.close = AsyncMock()
        await source.close()
        source.es_client.close.assert_called_once()
