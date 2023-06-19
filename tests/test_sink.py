#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import datetime
from copy import deepcopy
from unittest import mock
from unittest.mock import ANY, Mock, call

import pytest

from connectors.es.settings import TEXT_FIELD_MAPPING
from connectors.es.sink import (
    AsyncBulkRunningError,
    ContentIndexNameInvalid,
    Extractor,
    IndexMissing,
    Sink,
    SyncOrchestrator,
)
from connectors.protocol import JobType, Pipeline
from tests.commons import AsyncIterator

INDEX = "some-index"
TIMESTAMP = datetime.datetime(year=2023, month=1, day=1)
NO_FILTERING = ()
DOC_ONE_ID = 1

DOC_ONE = {"_id": DOC_ONE_ID, "_timestamp": TIMESTAMP}

DOC_ONE_DIFFERENT_TIMESTAMP = {
    "_id": DOC_ONE_ID,
    "_timestamp": TIMESTAMP + datetime.timedelta(days=1),
}

DOC_TWO = {"_id": 2, "_timestamp": TIMESTAMP}
DOC_THREE = {"_id": 3, "_timestamp": TIMESTAMP}

SYNC_RULES_ENABLED = True
SYNC_RULES_DISABLED = False
CONTENT_EXTRACTION_ENABLED = True
CONTENT_EXTRACTION_DISABLED = False


@pytest.mark.asyncio
async def test_prepare_content_index_raise_error_when_index_name_invalid():
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    es = SyncOrchestrator(config)

    with pytest.raises(ContentIndexNameInvalid):
        await es.prepare_content_index("lalalalalalalala woohooo")


@pytest.mark.asyncio
async def test_prepare_content_index_raise_error_when_index_does_not_exist(
    mock_responses,
):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )
    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=open",
        headers=headers,
        status=404,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index",
        payload={"_id": "1"},
        headers=headers,
    )

    es = SyncOrchestrator(config)

    with pytest.raises(IndexMissing):
        await es.prepare_content_index("search-new-index")

    await es.close()


@pytest.mark.asyncio
async def test_prepare_content_index(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    # prepare-index, with mappings
    dynamic_templates = {
        "data": {
            "match_mapping_type": "string",
            "mapping": TEXT_FIELD_MAPPING,
        }
    }
    mappings = {
        "dynamic": True,
        "dynamic_templates": dynamic_templates,
        "properties": {"name": {"type": "keyword"}},
    }
    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=open",
        headers=headers,
    )
    mock_responses.get(
        "http://nowhere.com:9200/search-new-index/_mapping?expand_wildcards=open",
        headers=headers,
        payload={"search-new-index": {"mappings": {}}},
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_mapping?expand_wildcards=open",
        headers=headers,
        payload=mappings,
    )

    es = SyncOrchestrator(config)
    put_mappings_result = asyncio.Future()
    put_mappings_result.set_result({"acknowledged": True})
    with mock.patch.object(
        es.client.indices,
        "put_mapping",
        return_value=put_mappings_result,
    ) as put_mapping_mock:
        index_name = "search-new-index"
        await es.prepare_content_index(index_name, mappings=mappings)

        await es.close()

        expected_params = {
            "index": index_name,
            "dynamic": mappings["dynamic"],
            "dynamic_templates": mappings["dynamic_templates"],
            "properties": mappings["properties"],
            "expand_wildcards": "open",
        }

        put_mapping_mock.assert_called_with(**expected_params)


def set_responses(mock_responses, ts=None):
    if ts is None:
        ts = datetime.datetime.now().isoformat()
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.get(
        "http://nowhere.com:9200/search-some-index",
        payload={"_id": "1"},
        headers=headers,
    )
    source_1 = {"id": "1", "_timestamp": ts}
    source_2 = {"id": "2", "_timestamp": ts}

    mock_responses.post(
        "http://nowhere.com:9200/search-some-index/_search?scroll=5m",
        payload={
            "_scroll_id": "1234",
            "_shards": {},
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            },
        },
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/_search/scroll",
        headers=headers,
    )
    mock_responses.post(
        "http://nowhere.com:9200/_search/scroll",
        payload={
            "hits": {
                "hits": [
                    {"_id": "1", "_source": source_1},
                    {"_id": "2", "_source": source_2},
                ]
            }
        },
        headers=headers,
    )

    mock_responses.put(
        "http://nowhere.com:9200/_bulk?pipeline=ent-search-generic-ingestion",
        payload={
            "took": 7,
            "errors": False,
            "items": [
                {
                    "index": {
                        "_index": "test",
                        "_id": "1",
                        "_version": 1,
                        "result": "created",
                        "forced_refresh": False,
                        "status": 200,
                    }
                },
                {
                    "delete": {
                        "_index": "test",
                        "_id": "3",
                        "result": "deleted",
                        "status": 200,
                    }
                },
                {
                    "update": {
                        "_index": "test",
                        "_id": "3",
                        "result": "updated",
                        "status": 200,
                    }
                },
            ],
        },
        headers=headers,
    )


@pytest.mark.asyncio
async def test_get_existing_ids(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = SyncOrchestrator(config)
    extractor = Extractor(es.client, None, "search-some-index")
    ids = []
    async for doc_id, _ in extractor._get_existing_ids():
        ids.append(doc_id)

    assert ids == ["1", "2"]
    await es.close()


@pytest.mark.asyncio
async def test_async_bulk(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = SyncOrchestrator(config)
    pipeline = Pipeline({})

    async def get_docs():
        async def _dl_none(doit=True, timestamp=None):
            return None

        async def _dl(doit=True, timestamp=None):
            if not doit:
                return
            return {"TEXT": "DATA", "_timestamp": timestamp, "_id": "1"}

        yield {
            "_id": "1",
            "_timestamp": datetime.datetime.now().isoformat(),
        }, _dl, "index"
        yield {"_id": "3"}, _dl_none, "index"

    await es.async_bulk("search-some-index", get_docs(), pipeline, JobType.FULL)
    while not es.done():
        await asyncio.sleep(0.1)

    ingestion_stats = es.ingestion_stats()

    assert ingestion_stats == {
        "doc_created": 1,
        "attachment_extracted": 1,
        "doc_updated": 1,
        "doc_deleted": 1,
        "bulk_operations": {"index": 2, "delete": 1},
        "indexed_document_count": 2,
        "indexed_document_volume": ANY,
        "deleted_document_count": 1,
    }

    # 2nd sync
    set_responses(mock_responses)
    with pytest.raises(AsyncBulkRunningError):
        await es.async_bulk("search-some-index", get_docs(), pipeline, JobType.FULL)

    await es.close()


def index_operation(doc):
    # deepcopy as get_docs mutates docs
    doc_copy = deepcopy(doc)
    doc_id = doc_copy["id"] = doc_copy.pop("_id")

    return {"_op_type": "index", "_index": INDEX, "_id": doc_id, "doc": doc_copy}


def update_operation(doc):
    # deepcopy as get_docs mutates docs
    doc_copy = deepcopy(doc)
    doc_id = doc_copy["id"] = doc_copy.pop("_id")

    return {"_op_type": "update", "_index": INDEX, "_id": doc_id, "doc": doc_copy}


def delete_operation(doc):
    return {"_op_type": "delete", "_index": INDEX, "_id": doc["_id"]}


def end_docs_operation():
    return "END_DOCS"


def created(doc_count):
    """Used for test readability."""
    return doc_count


def updated(doc_count):
    """Used for test readability."""
    return doc_count


def deleted(doc_count):
    """Used for test readability."""
    return doc_count


def total_downloads(count):
    """Used for test readability."""
    return count


async def queue_mock():
    queue = Mock()
    future = asyncio.Future()
    future.set_result(1)
    queue.put = Mock(return_value=future)
    return queue


def lazy_download_fake(doc):
    async def lazy_download(**kwargs):
        # also deepcopy to prevent side effects as docs get mutated in get_docs
        return deepcopy(doc)

    return lazy_download


def queue_called_with_operations(queue, operations):
    expected_calls = [call(operation) for operation in operations]
    actual_calls = queue.put.call_args_list

    return actual_calls == expected_calls and queue.put.call_count == len(
        expected_calls
    )


async def basic_rule_engine_mock(return_values):
    basic_rule_engine = Mock()
    basic_rule_engine.should_ingest = (
        Mock(side_effect=list(return_values))
        if len(return_values)
        else Mock(return_value=True)
    )
    return basic_rule_engine


async def lazy_downloads_mock():
    lazy_downloads = Mock()
    future = asyncio.Future()
    future.set_result(1)
    lazy_downloads.put = Mock(return_value=future)
    return lazy_downloads


async def setup_extractor(
    queue,
    basic_rule_engine=None,
    sync_rules_enabled=False,
    content_extraction_enabled=False,
):
    # filtering content doesn't matter as the BasicRuleEngine behavior is mocked
    filter_mock = Mock()
    filter_mock.get_active_filter = Mock(return_value={})
    extractor = Extractor(
        None,
        queue,
        INDEX,
        filter_=filter_mock,
        content_extraction_enabled=content_extraction_enabled,
    )
    extractor.basic_rule_engine = basic_rule_engine if sync_rules_enabled else None
    return extractor


@pytest.mark.parametrize(
    "existing_docs, docs_from_source, doc_should_ingest, sync_rules_enabled, content_extraction_enabled, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted, expected_total_downloads",
    [
        (
            # no docs exist, data source has doc 1 -> ingest doc 1
            [],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, nothing in source -> delete one doc
            [DOC_ONE],
            [],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 2 is present, data source only returns doc 1 -> delete doc 2 and ingest doc 1
            [DOC_TWO],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source also has doc 1 with the same timestamp -> nothing happens
            [DOC_ONE],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1 with different timestamp -> doc is updated
            [DOC_ONE],
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # update happens through overwriting
                index_operation(DOC_ONE_DIFFERENT_TIMESTAMP),
                end_docs_operation(),
            ],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should not ingest doc 1
                end_docs_operation()
            ],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1, doc 1 should be filtered -> delete doc 1
            [DOC_ONE],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                delete_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1 (different timestamp), doc 1 should be filtered -> delete doc 1
            [DOC_ONE],
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None, "index")],
            # still filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filtering throws exception
            [Exception()],
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            ["FETCH_ERROR"],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [],
            # no doc present, lazy download for doc 1 specified -> index and increment the downloads counter
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(1),
        ),
        (
            # doc 1 present, data source has doc 1 -> no lazy download if timestamps are the same for the docs
            [DOC_ONE],
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 present, data source has doc 1 with different timestamp
            # lazy download coroutine present -> execute lazy download
            [DOC_ONE],
            [
                (
                    DOC_ONE_DIFFERENT_TIMESTAMP,
                    lazy_download_fake(DOC_ONE_DIFFERENT_TIMESTAMP),
                    "index",
                )
            ],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE_DIFFERENT_TIMESTAMP), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(1),
        ),
        (
            [],
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_DISABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should ingest doc 1 as sync rules are disabled
                index_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # content_extraction_enabled is false and no download is provided,
            # indexing should still work, but nothing should be downloaded
            [],
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            # content_extraction_enabled is false but a download is also provided,
            # indexing should still work, but nothing should be downloaded
            [],
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
    ],
)
@mock.patch("connectors.es.sink.Extractor._get_existing_ids")
@pytest.mark.asyncio
async def test_get_docs(
    get_existing_ids,
    existing_docs,
    docs_from_source,
    doc_should_ingest,
    sync_rules_enabled,
    content_extraction_enabled,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
    expected_total_downloads,
):
    lazy_downloads = await lazy_downloads_mock()

    get_existing_ids.return_value = AsyncIterator(
        [(doc["_id"], doc["_timestamp"]) for doc in existing_docs]
    )

    with mock.patch("connectors.utils.ConcurrentTasks", return_value=lazy_downloads):
        queue = await queue_mock()
        basic_rule_engine = await basic_rule_engine_mock(doc_should_ingest)

        # deep copying docs is needed as get_docs mutates the document ids which has side effects on other test
        # instances
        doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

        extractor = await setup_extractor(
            queue,
            basic_rule_engine=basic_rule_engine,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
        )

        await extractor.run(doc_generator, JobType.FULL)

        assert extractor.total_docs_updated == expected_total_docs_updated
        assert extractor.total_docs_created == expected_total_docs_created
        assert extractor.total_docs_deleted == expected_total_docs_deleted
        assert extractor.total_downloads == expected_total_downloads

        assert queue_called_with_operations(queue, expected_queue_operations)


@pytest.mark.parametrize(
    "docs_from_source, doc_should_ingest, sync_rules_enabled, content_extraction_enabled, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted, expected_total_downloads",
    [
        (
            [],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "update")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [update_operation(DOC_ONE), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "delete")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index"), (DOC_TWO, None, "delete")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should not ingest doc 1
                end_docs_operation()
            ],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filtering throws exception
            [Exception()],
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            ["FETCH_ERROR"],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # no doc present, lazy download for doc 1 specified -> index and increment the downloads counter
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(1),
        ),
        (
            [(DOC_ONE, None, "index")],
            # filter out doc 1
            (False,),
            SYNC_RULES_DISABLED,
            CONTENT_EXTRACTION_ENABLED,
            [
                # should ingest doc 1 as sync rules are disabled
                index_operation(DOC_ONE),
                end_docs_operation(),
            ],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, None, "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
        (
            [(DOC_ONE, lazy_download_fake(DOC_ONE), "index")],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            CONTENT_EXTRACTION_DISABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(0),
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_incrementally(
    docs_from_source,
    doc_should_ingest,
    sync_rules_enabled,
    content_extraction_enabled,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
    expected_total_downloads,
):
    lazy_downloads = await lazy_downloads_mock()

    with mock.patch("connectors.utils.ConcurrentTasks", return_value=lazy_downloads):
        queue = await queue_mock()
        basic_rule_engine = await basic_rule_engine_mock(doc_should_ingest)

        # deep copying docs is needed as get_docs mutates the document ids which has side effects on other test
        # instances
        doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

        extractor = await setup_extractor(
            queue,
            basic_rule_engine=basic_rule_engine,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
        )

        await extractor.run(doc_generator, JobType.INCREMENTAL)

        assert extractor.total_docs_updated == expected_total_docs_updated
        assert extractor.total_docs_created == expected_total_docs_created
        assert extractor.total_docs_deleted == expected_total_docs_deleted
        assert extractor.total_downloads == expected_total_downloads

        assert queue_called_with_operations(queue, expected_queue_operations)


@pytest.mark.parametrize(
    "existing_docs, docs_from_source, expected_queue_operations, "
    "expected_total_docs_updated, expected_total_docs_created, expected_total_docs_deleted",
    [
        (
            [],
            [],
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
        ),
        (
            [],
            [(DOC_ONE, None, None)],
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP],
            [(DOC_ONE, None, None)],
            [update_operation(DOC_ONE), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
        ),
        (
            [DOC_ONE],
            [],
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
        ),
        (
            [DOC_TWO],
            [(DOC_ONE, None, None)],
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP, DOC_TWO],
            [(DOC_ONE, None, None)],
            [
                update_operation(DOC_ONE),
                delete_operation(DOC_TWO),
                end_docs_operation(),
            ],
            updated(1),
            created(0),
            deleted(1),
        ),
        (
            [DOC_ONE_DIFFERENT_TIMESTAMP, DOC_TWO],
            [(DOC_ONE, None, None), (DOC_THREE, None, None)],
            [
                update_operation(DOC_ONE),
                index_operation(DOC_THREE),
                delete_operation(DOC_TWO),
                end_docs_operation(),
            ],
            updated(1),
            created(1),
            deleted(1),
        ),
    ],
)
@mock.patch("connectors.es.sink.Extractor._get_existing_ids")
@pytest.mark.asyncio
async def test_get_access_control_docs(
    get_existing_ids,
    existing_docs,
    docs_from_source,
    expected_queue_operations,
    expected_total_docs_updated,
    expected_total_docs_created,
    expected_total_docs_deleted,
):
    get_existing_ids.return_value = AsyncIterator(
        [(doc["_id"], doc["_timestamp"]) for doc in existing_docs]
    )

    queue = await queue_mock()
    # deep copying docs is needed as get_access_control_docs mutates the document ids which has side effects on other test
    # instances
    doc_generator = AsyncIterator([deepcopy(doc) for doc in docs_from_source])

    extractor = await setup_extractor(queue=queue)

    await extractor.run(doc_generator, JobType.ACCESS_CONTROL)

    assert extractor.total_docs_updated == expected_total_docs_updated
    assert extractor.total_docs_created == expected_total_docs_created
    assert extractor.total_docs_deleted == expected_total_docs_deleted

    assert queue_called_with_operations(queue, expected_queue_operations)


STATS = {
    "index": {"1": 1},
    "update": {"2": 1},
    "delete": {"3": 0},
}

INDEX_ITEM = {"index": {"_id": "1", "result": "created"}}
FAILED_INDEX_ITEM = {"index": {"_id": "1"}}
UPDATE_ITEM = {"update": {"_id": "2", "result": "updated"}}
FAILED_UPDATE_ITEM = {"update": {"_id": "2"}}
DELETE_ITEM = {"delete": {"_id": "3", "result": "deleted"}}
FAILED_DELETE_ITEM = {"delete": {"_id": "3"}}


@pytest.mark.parametrize(
    "res, expected_result",
    [
        (
            {"items": [INDEX_ITEM, UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 2,
                "indexed_document_volume": 2,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [INDEX_ITEM, FAILED_UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [INDEX_ITEM, UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 2,
                "indexed_document_volume": 2,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [INDEX_ITEM, FAILED_UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 1,
                "indexed_document_volume": 1,
                "deleted_document_count": 0,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, FAILED_UPDATE_ITEM, DELETE_ITEM]},
            {
                "indexed_document_count": 0,
                "indexed_document_volume": 0,
                "deleted_document_count": 1,
            },
        ),
        (
            {"items": [FAILED_INDEX_ITEM, FAILED_UPDATE_ITEM, FAILED_DELETE_ITEM]},
            {
                "indexed_document_count": 0,
                "indexed_document_volume": 0,
                "deleted_document_count": 0,
            },
        ),
    ],
)
def test_bulk_populate_stats(res, expected_result):
    sink = Sink(
        client=None,
        queue=None,
        chunk_size=0,
        pipeline=None,
        chunk_mem_size=0,
        max_concurrency=0,
    )
    sink._populate_stats(deepcopy(STATS), res)

    assert sink.indexed_document_count == expected_result["indexed_document_count"]
    assert sink.indexed_document_volume == expected_result["indexed_document_volume"]
    assert sink.deleted_document_count == expected_result["deleted_document_count"]


@pytest.mark.parametrize(
    "extractor_task, extractor_task_done, sink_task, sink_task_done, expected_result",
    [
        (None, False, None, False, True),
        (Mock(), False, None, False, False),
        (Mock(), True, None, False, True),
        (None, False, Mock(), False, False),
        (None, False, Mock(), True, True),
        (Mock(), False, Mock(), False, False),
        (Mock(), False, Mock(), True, False),
        (Mock(), True, Mock(), False, False),
        (Mock(), True, Mock(), True, True),
    ],
)
@pytest.mark.asyncio
async def test_elastic_server_done(
    extractor_task, extractor_task_done, sink_task, sink_task_done, expected_result
):
    if extractor_task is not None:
        extractor_task.done.return_value = extractor_task_done
    if sink_task is not None:
        sink_task.done.return_value = sink_task_done

    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    es = SyncOrchestrator(config)
    es._extractor_task = extractor_task
    es._sink_task = sink_task

    assert es.done() == expected_result

    await es.close()
