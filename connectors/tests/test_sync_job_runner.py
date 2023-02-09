#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from unittest.mock import ANY, AsyncMock, Mock

import pytest

from connectors.byoc import Filter, Pipeline
from connectors.filtering.validation import (
    FilteringValidationResult,
    FilteringValidationState,
)
from connectors.sync_job_runner import SyncJobRunner

total_document_count = 100


def create_runner(
    source_changed=True,
    source_available=True,
    filtering_valid=True,
    async_bulk_result={},
):
    source_klass = Mock()
    data_provider = Mock()
    data_provider.tweak_bulk_options = Mock()
    data_provider.changed = AsyncMock(return_value=source_changed)
    data_provider.ping = AsyncMock()
    if not source_available:
        data_provider.ping.side_effect = Exception()
    validation_result = FilteringValidationResult(
        state=FilteringValidationState.VALID
        if filtering_valid
        else FilteringValidationState.INVALID
    )
    data_provider.validate_filtering = AsyncMock(return_value=validation_result)
    source_klass.return_value = data_provider

    sync_job = Mock()
    sync_job.id = "1"
    sync_job.configuration = {}
    sync_job.service_type = "mysql"
    sync_job.index_name = "search-mysql"
    sync_job.pipeline = Pipeline({})
    sync_job.filtering = Filter()
    sync_job.claim = AsyncMock()
    sync_job.done = AsyncMock()
    sync_job.fail = AsyncMock()
    sync_job.reload = AsyncMock(return_value=sync_job)

    connector = Mock()
    connector.id = "1"
    connector.features.sync_rules_enabled.return_value = True
    connector.document_count = AsyncMock(return_value=total_document_count)
    connector.sync_starts = AsyncMock()
    connector.sync_done = AsyncMock()

    elastic_server = Mock()
    elastic_server.prepare_content_index = AsyncMock()
    elastic_server.async_bulk = AsyncMock(return_value=async_bulk_result)

    bulk_options = {}

    return SyncJobRunner(
        source_klass=source_klass,
        sync_job=sync_job,
        connector=connector,
        elastic_server=elastic_server,
        bulk_options=bulk_options,
    )


@pytest.mark.asyncio
async def test_job_claim_fail(patch_logger):
    service = create_runner()
    service.sync_job.claim.side_effect = Exception()
    await service.execute()

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_not_called()
    service.elastic_server.async_bulk.assert_not_called()
    service.sync_job.done.assert_not_called()
    service.sync_job.fail.assert_not_called()
    service.connector.sync_done.assert_not_called()


@pytest.mark.asyncio
async def test_connector_starts_fail(patch_logger):
    service = create_runner()
    service.connector.sync_starts.side_effect = Exception()
    await service.execute()

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_not_called()
    service.sync_job.done.assert_not_called()
    service.sync_job.fail.assert_not_called()
    service.connector.sync_done.assert_not_called()


@pytest.mark.asyncio
async def test_source_not_changed(patch_logger):
    service = create_runner(source_changed=False)
    await service.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_not_called()
    service.sync_job.done.assert_called_with(ingestion_stats=ingestion_stats)
    service.sync_job.fail.assert_not_called()
    service.connector.sync_done.assert_called_with(service.sync_job)


@pytest.mark.asyncio
async def test_source_not_available(patch_logger):
    service = create_runner(source_available=False)
    await service.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_not_called()
    service.sync_job.done.assert_not_called
    service.sync_job.fail.assert_called_with(ANY, ingestion_stats=ingestion_stats)
    service.connector.sync_done.assert_called_with(service.sync_job)


@pytest.mark.asyncio
async def test_invalid_filtering(patch_logger):
    service = create_runner(filtering_valid=False)
    await service.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_not_called()
    service.sync_job.done.assert_not_called
    service.sync_job.fail.assert_called_with(ANY, ingestion_stats=ingestion_stats)
    service.connector.sync_done.assert_called_with(service.sync_job)


@pytest.mark.asyncio
async def test_async_bulk_error(patch_logger):
    error = "something wrong"
    async_bulk_result = {"fetch_error": error}
    service = create_runner(async_bulk_result=async_bulk_result)
    await service.execute()

    ingestion_stats = {
        "indexed_document_count": 0,
        "indexed_document_volume": 0,
        "deleted_document_count": 0,
        "total_document_count": total_document_count,
    }

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_called()
    service.sync_job.done.assert_not_called
    service.sync_job.fail.assert_called_with(error, ingestion_stats=ingestion_stats)
    service.connector.sync_done.assert_called_with(service.sync_job)


@pytest.mark.asyncio
async def test_sync_job_runner(patch_logger):
    doc_updated = 10
    doc_created = 15
    doc_deleted = 20
    async_bulk_result = {
        "doc_updated": doc_updated,
        "doc_created": doc_created,
        "doc_deleted": doc_deleted,
    }
    service = create_runner(async_bulk_result=async_bulk_result)
    await service.execute()

    ingestion_stats = {
        "indexed_document_count": doc_updated + doc_created,
        "indexed_document_volume": 0,
        "deleted_document_count": doc_deleted,
        "total_document_count": total_document_count,
    }

    service.sync_job.claim.assert_called()
    service.connector.sync_starts.assert_called()
    service.elastic_server.async_bulk.assert_called()
    service.sync_job.done.assert_called_with(ingestion_stats=ingestion_stats)
    service.sync_job.fail.assert_not_called
    service.connector.sync_done.assert_called_with(service.sync_job)
