#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import datetime
from copy import deepcopy
from unittest import mock
from unittest.mock import Mock, call

import pytest

from connectors.byoc import Pipeline
from connectors.byoei import (
    ContentIndexNameInvalid,
    ElasticServer,
    Fetcher,
    IndexMissing,
)
from connectors.tests.commons import AsyncIterator

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

SYNC_RULES_ENABLED = True
SYNC_RULES_DISABLED = False


@pytest.mark.asyncio
async def test_prepare_content_index_raise_error_when_index_name_invalid():
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    es = ElasticServer(config)

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

    es = ElasticServer(config)

    with pytest.raises(IndexMissing):
        await es.prepare_content_index("search-new-index")


@pytest.mark.asyncio
async def test_prepare_content_index(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    # prepare-index, with mappings
    mappings = {"properties": {"name": {"type": "keyword"}}}
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
    )

    es = ElasticServer(config)

    await es.prepare_content_index("search-new-index", mappings=mappings)

    await es.close()


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
                {"delete": {"_index": "test", "_id": "3", "status": 200}},
                {"create": {"_index": "test", "_id": "3", "status": 200}},
            ],
        },
        headers=headers,
    )


@pytest.mark.asyncio
async def test_get_existing_ids(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = ElasticServer(config)
    ids = []
    async for doc_id, ts in es.get_existing_ids("search-some-index"):
        ids.append(doc_id)

    assert ids == ["1", "2"]
    await es.close()


@pytest.mark.asyncio
async def test_async_bulk(mock_responses, patch_logger):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses)

    es = ElasticServer(config)
    pipeline = Pipeline({})

    async def get_docs():
        async def _dl_none(doit=True, timestamp=None):
            return None

        async def _dl(doit=True, timestamp=None):
            if not doit:
                return
            return {"TEXT": "DATA", "_timestamp": timestamp, "_id": "1"}

        yield {"_id": "1", "_timestamp": datetime.datetime.now().isoformat()}, _dl
        yield {"_id": "3"}, _dl_none

    res = await es.async_bulk("search-some-index", get_docs(), pipeline)

    assert res == {
        "bulk_operations": {"index": 2, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 1,
        "doc_updated": 1,
        "fetch_error": None,
    }

    # two syncs
    set_responses(mock_responses)
    res = await es.async_bulk("search-some-index", get_docs(), pipeline)

    assert res == {
        "bulk_operations": {"index": 2, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 1,
        "doc_updated": 1,
        "fetch_error": None,
    }

    await es.close()


@pytest.mark.asyncio
async def test_async_bulk_same_ts(mock_responses, patch_logger):
    ts = datetime.datetime.now().isoformat()
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    set_responses(mock_responses, ts)
    es = ElasticServer(config)
    pipeline = Pipeline({})

    async def get_docs():
        async def _dl(doit=True, timestamp=None):
            if not doit:
                return  # Canceled
            return {"TEXT": "DATA", "_timestamp": timestamp, "_id": "1"}

        yield {"_id": "1", "_timestamp": ts}, _dl
        yield {"_id": "3", "_timestamp": ts}, None

    res = await es.async_bulk("search-some-index", get_docs(), pipeline)

    assert res == {
        "bulk_operations": {"index": 1, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 0,
        "doc_updated": 0,
        "fetch_error": None,
    }

    set_responses(mock_responses, ts)
    res = await es.async_bulk("search-some-index", get_docs(), pipeline)

    assert res == {
        "bulk_operations": {"index": 1, "delete": 1},
        "doc_created": 1,
        "doc_deleted": 1,
        "attachment_extracted": 0,
        "doc_updated": 0,
        "fetch_error": None,
    }

    await es.close()


def index_operation(doc):
    # deepcopy as get_docs mutates docs
    doc_copy = deepcopy(doc)
    doc_id = doc_copy["id"] = doc_copy.pop("_id")

    return {"_op_type": "index", "_index": INDEX, "_id": doc_id, "doc": doc_copy}


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


async def setup_fetcher(basic_rule_engine, existing_docs, queue, sync_rules_enabled):
    client = Mock()
    existing_ids = {doc["_id"]: doc["_timestamp"] for doc in existing_docs}

    # filtering content doesn't matter as the BasicRuleEngine behavior is mocked
    filter_mock = Mock()
    filter_mock.get_active_filter = Mock(return_value={})
    fetcher = Fetcher(client, queue, INDEX, existing_ids, filter_=filter_mock)
    fetcher.basic_rule_engine = basic_rule_engine if sync_rules_enabled else None
    return fetcher


@pytest.mark.parametrize(
    "existing_docs, docs_from_source, doc_should_ingest, sync_rules_enabled, expected_queue_operations, expected_total_docs_updated, "
    "expected_total_docs_created, expected_total_docs_deleted, expected_total_downloads",
    [
        (
            # no docs exist, data source has doc 1 -> ingest doc 1
            [],
            [(DOC_ONE, None)],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
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
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 2 is present, data source only returns doc 1 -> delete doc 2 and ingest doc 1
            [DOC_TWO],
            [(DOC_ONE, None)],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            [index_operation(DOC_ONE), delete_operation(DOC_TWO), end_docs_operation()],
            updated(0),
            created(1),
            deleted(1),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source also has doc 1 with the same timestamp -> nothing happens
            [DOC_ONE],
            [(DOC_ONE, None)],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            [end_docs_operation()],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            # doc 1 is present, data source has doc 1 with different timestamp -> doc is updated
            [DOC_ONE],
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None)],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
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
            [(DOC_ONE, None)],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
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
            [(DOC_ONE, None)],
            # filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
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
            [(DOC_ONE_DIFFERENT_TIMESTAMP, None)],
            # still filter out doc 1
            (False,),
            SYNC_RULES_ENABLED,
            [delete_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(0),
            deleted(1),
            total_downloads(0),
        ),
        (
            [],
            [(DOC_ONE, None)],
            # filtering throws exception
            [Exception()],
            SYNC_RULES_ENABLED,
            ["FETCH_ERROR"],
            updated(0),
            created(0),
            deleted(0),
            total_downloads(0),
        ),
        (
            [],
            # no doc present, lazy download for doc 1 specified -> index and increment the downloads counter
            [(DOC_ONE, lazy_download_fake(DOC_ONE))],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            [index_operation(DOC_ONE), end_docs_operation()],
            updated(0),
            created(1),
            deleted(0),
            total_downloads(1),
        ),
        (
            # doc 1 present, data source has doc 1 -> no lazy download if timestamps are the same for the docs
            [DOC_ONE],
            [(DOC_ONE, lazy_download_fake(DOC_ONE))],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
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
                )
            ],
            NO_FILTERING,
            SYNC_RULES_ENABLED,
            [index_operation(DOC_ONE_DIFFERENT_TIMESTAMP), end_docs_operation()],
            updated(1),
            created(0),
            deleted(0),
            total_downloads(1),
        ),
        (
            [],
            [(DOC_ONE, None)],
            # filter out doc 1
            (False,),
            SYNC_RULES_DISABLED,
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
    ],
)
@pytest.mark.asyncio
async def test_get_docs(
    existing_docs,
    docs_from_source,
    doc_should_ingest,
    sync_rules_enabled,
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

        fetcher = await setup_fetcher(
            basic_rule_engine, existing_docs, queue, sync_rules_enabled
        )

        await fetcher.get_docs(doc_generator)

        assert fetcher.total_docs_updated == expected_total_docs_updated
        assert fetcher.total_docs_created == expected_total_docs_created
        assert fetcher.total_docs_deleted == expected_total_docs_deleted
        assert fetcher.total_downloads == expected_total_downloads

        assert queue_called_with_operations(queue, expected_queue_operations)
