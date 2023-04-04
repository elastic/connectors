#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOEI protocol (+some ids collecting)

`ElasticServer` is orchestrating a sync by:

- creating a queue
- launching a `Fetcher`, a wrapper on the top of the documents' generator
- launching a `Bulker`, a class that aggregates documents and run the bulk API


                  ElasticServer.async_bulk(generator)
                               |
                               |
Elasticsearch <== Bulker <== queue <== Fetcher <== generator

"""
import asyncio
import copy
import functools
import logging
import time
from collections import defaultdict

from elasticsearch import NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan

from connectors.byoc import Filter
from connectors.es import ESClient
from connectors.filtering.basic_rule import BasicRuleEngine, parse
from connectors.logger import logger
from connectors.utils import (
    DEFAULT_CHUNK_MEM_SIZE,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_CONCURRENT_DOWNLOADS,
    DEFAULT_DISPLAY_EVERY,
    DEFAULT_MAX_CONCURRENCY,
    DEFAULT_QUEUE_MEM_SIZE,
    DEFAULT_QUEUE_SIZE,
    ConcurrentTasks,
    MemQueue,
    get_size,
    iso_utc,
)

__all__ = ["ElasticServer"]

OP_INDEX = "index"
OP_UPSERT = "update"
OP_DELETE = "delete"
TIMESTAMP_FIELD = "_timestamp"


def get_mb_size(ob):
    """Returns the size of ob in MiB"""
    return round(get_size(ob) / (1024 * 1024), 2)


class Bulker:
    """Send bulk operations in batches by consuming a queue.

    This class runs a coroutine that gets operations out of a `queue` and collects them to
    build and send bulk requests using a `client`

    Arguments:

    - `client` -- an instance of `connectors.es.ESClient`
    - `queue` -- an instance of `asyncio.Queue` to pull docs from
    - `chunk_size` -- a maximum number of operations to send per request
    - `pipeline` -- ingest pipeline settings to pass to the bulk API
    - `chunk_mem_size` -- a maximum size in MiB for each bulk request
    - `max_concurrency` -- a maximum number of concurrent bulk requests
    """

    def __init__(
        self,
        client,
        queue,
        chunk_size,
        pipeline,
        chunk_mem_size,
        max_concurrency,
    ):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.ops = defaultdict(int)
        self.chunk_size = chunk_size
        self.pipeline = pipeline
        self.chunk_mem_size = chunk_mem_size * 1024 * 1024
        self.max_concurrent_bulks = max_concurrency
        self.bulk_tasks = ConcurrentTasks(max_concurrency=max_concurrency)
        self.indexed_document_count = 0
        self.indexed_document_volume = 0
        self.deleted_document_count = 0

    def _bulk_op(self, doc, operation=OP_INDEX):
        doc_id = doc["_id"]
        index = doc["_index"]

        if operation == OP_INDEX:
            return [{operation: {"_index": index, "_id": doc_id}}, doc["doc"]]
        if operation == OP_UPSERT:
            return [
                {operation: {"_index": index, "_id": doc_id}},
                {"doc": doc["doc"], "doc_as_upsert": True},
            ]
        if operation == OP_DELETE:
            return [{operation: {"_index": index, "_id": doc_id}}]

        raise TypeError(operation)

    async def _batch_bulk(self, operations, stats):
        # TODO: treat result to retry errors like in async_streaming_bulk
        task_num = len(self.bulk_tasks)

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                f"Task {task_num} - Sending a batch of {len(operations)} ops -- {get_mb_size(operations)}MiB"
            )
        start = time.time()
        try:
            res = await self.client.bulk(
                operations=operations, pipeline=self.pipeline["name"]
            )

            if res.get("errors"):
                for item in res["items"]:
                    for op, data in item.items():
                        if "error" in data:
                            logger.error(f"operation {op} failed, {data['error']}")
                            raise Exception(data["error"]["reason"])

            self._populate_stats(stats, res)

        finally:
            self.bulk_time += time.time() - start

        return res

    def _populate_stats(self, stats, res):
        for item in res["items"]:
            for op, data in item.items():
                # "result" is only present in successful operations
                if "result" not in data:
                    del stats[op][data["_id"]]

        self.indexed_document_count += len(stats[OP_INDEX]) + len(stats[OP_UPSERT])
        self.indexed_document_volume += sum(stats[OP_INDEX].values()) + sum(
            stats[OP_UPSERT].values()
        )
        self.deleted_document_count += len(stats[OP_DELETE])

        logger.debug(
            f"Bulker stats - no. of docs indexed: {self.indexed_document_count}, volume of docs indexed: {round(self.indexed_document_volume)} bytes, no. of docs deleted: {self.deleted_document_count}"
        )

    async def run(self):
        try:
            await self._run()
        except asyncio.CancelledError:
            logger.info("Task is canceled, stop Bulker...")
            raise

    async def _run(self):
        """Creates batches of bulk calls given a queue of items.

        An item is a (size, object) tuple. Exits when the
        item is the `END_DOCS` or `FETCH_ERROR` string.

        Bulk calls are executed concurrently with a maximum number of concurrent
        requests.
        """
        batch = []
        # stats is a dictionary containing stats for 3 operations. In each sub-dictionary, it is a doc id to size map.
        stats = {OP_INDEX: {}, OP_UPSERT: {}, OP_DELETE: {}}
        self.bulk_time = 0
        self.bulking = True
        bulk_size = 0
        overhead_size = None

        while True:
            doc_size, doc = await self.queue.get()
            if doc in ("END_DOCS", "FETCH_ERROR"):
                break
            operation = doc["_op_type"]
            doc_id = doc["_id"]
            if operation == OP_DELETE:
                stats[operation][doc_id] = 0
            else:
                # the doc_size also includes _op_type, _index and _id,
                # which we want to exclude when calculating the size.
                if overhead_size is None:
                    overhead = {
                        "_op_type": operation,
                        "_index": doc["_index"],
                        "_id": doc_id,
                    }
                    overhead_size = get_size(overhead)
                stats[operation][doc_id] = max(doc_size - overhead_size, 0)
            self.ops[operation] += 1
            batch.extend(self._bulk_op(doc, operation))

            bulk_size += doc_size
            if len(batch) >= self.chunk_size or bulk_size > self.chunk_mem_size:
                await self.bulk_tasks.put(
                    functools.partial(
                        self._batch_bulk,
                        copy.copy(batch),
                        copy.copy(stats),
                    )
                )
                batch.clear()
                stats = {OP_INDEX: {}, OP_UPSERT: {}, OP_DELETE: {}}
                bulk_size = 0

            await asyncio.sleep(0)

        await self.bulk_tasks.join()
        if len(batch) > 0:
            await self._batch_bulk(batch, stats)


class Fetcher:
    """Grabs data and adds them in the queue for the bulker.

    This class runs a coroutine that puts docs in `queue`, given a document generator.

    Arguments:
    - queue: an `asyncio.Queue` to put docs in
    - index: the target Elasticsearch index
    - existing_ids: a list of existing Elasticsearch document ids found in the index
    - filter_: an instance of `Filter` to apply on the fetched document -- default: `None`
    - sync_rules_enabled: if `True`, we apply rules -- default: `False`
    - content_extraction_enabled: if `True`, download content -- default `True`
    - display_every -- display a log every `display_every` doc -- default: `DEFAULT_DISPLAY_EVERY`
    - concurrent_downloads: -- concurrency level for downloads -- default: `DEFAULT_CONCURRENT_DOWNLOADS`
    """

    def __init__(
        self,
        queue,
        index,
        existing_ids,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        display_every=DEFAULT_DISPLAY_EVERY,
        concurrent_downloads=DEFAULT_CONCURRENT_DOWNLOADS,
    ):
        if filter_ is None:
            filter_ = Filter()
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.index = index
        self.loop = asyncio.get_event_loop()
        self.existing_ids = existing_ids
        self.sync_runs = False
        self.total_downloads = 0
        self.total_docs_updated = 0
        self.total_docs_created = 0
        self.total_docs_deleted = 0
        self.fetch_error = None
        self.filter_ = filter_
        self.basic_rule_engine = (
            BasicRuleEngine(parse(filter_.basic_rules)) if sync_rules_enabled else None
        )
        self.content_extraction_enabled = content_extraction_enabled
        self.display_every = display_every
        self.concurrent_downloads = concurrent_downloads

    def __str__(self):
        return (
            "Fetcher <"
            f"create: {self.total_docs_created} |"
            f"update: {self.total_docs_updated} |"
            f"delete: {self.total_docs_deleted}>"
        )

    async def _deferred_index(self, lazy_download, doc_id, doc, operation):
        data = await lazy_download(doit=True, timestamp=doc[TIMESTAMP_FIELD])

        if data is not None:
            self.total_downloads += 1
            data.pop("_id", None)
            data.pop(TIMESTAMP_FIELD, None)
            doc.update(data)

        await self.queue.put(
            {
                "_op_type": operation,
                "_index": self.index,
                "_id": doc_id,
                "doc": doc,
            }
        )

    async def run(self, generator):
        try:
            await self.get_docs(generator)
        except asyncio.CancelledError:
            logger.info("Task is canceled, stop Fetcher...")
            raise

    async def get_docs(self, generator):
        """Iterate on a generator of documents to fill a queue of bulk operations for the `Bulker` to consume.

        A document might be discarded if its timestamp has not changed.
        Extraction happens in a separate task, when a document contains files.
        """
        logger.info("Starting doc lookups")
        self.sync_runs = True
        count = 0
        lazy_downloads = ConcurrentTasks(self.concurrent_downloads)
        try:
            async for doc in generator:
                doc, lazy_download = doc
                count += 1
                if count % self.display_every == 0:
                    logger.info(str(self))

                doc_id = doc["id"] = doc.pop("_id")

                if self.basic_rule_engine and not self.basic_rule_engine.should_ingest(
                    doc
                ):
                    continue

                if doc_id in self.existing_ids:
                    # pop out of self.existing_ids
                    ts = self.existing_ids.pop(doc_id)

                    # If the doc has a timestamp, we can use it to see if it has
                    # been modified. This reduces the bulk size a *lot*
                    #
                    # Some backends do not know how to do this, so it's optional.
                    # For these, we update the docs in any case.
                    if TIMESTAMP_FIELD in doc and ts == doc[TIMESTAMP_FIELD]:
                        # cancel the download
                        if (
                            self.content_extraction_enabled
                            and lazy_download is not None
                        ):
                            await lazy_download(doit=False)
                        continue

                    # the doc exists, but we are still overwriting it with `index`
                    operation = OP_INDEX
                    self.total_docs_updated += 1
                else:
                    operation = OP_INDEX
                    self.total_docs_created += 1
                    if TIMESTAMP_FIELD not in doc:
                        doc[TIMESTAMP_FIELD] = iso_utc()

                # if we need to call lazy_download we push it in lazy_downloads
                if self.content_extraction_enabled and lazy_download is not None:
                    await lazy_downloads.put(
                        functools.partial(
                            self._deferred_index, lazy_download, doc_id, doc, operation
                        )
                    )

                else:
                    # we can push into the queue right away
                    await self.queue.put(
                        {
                            "_op_type": operation,
                            "_index": self.index,
                            "_id": doc_id,
                            "doc": doc,
                        }
                    )

                await asyncio.sleep(0)
        except Exception as e:
            logger.critical("The document fetcher failed", exc_info=True)
            await self.queue.put("FETCH_ERROR")
            self.fetch_error = e
            return
        finally:
            # wait for all downloads to be finished
            await lazy_downloads.join()

        # We delete any document that existed in Elasticsearch that was not
        # returned by the backend.
        #
        # Since we popped out every seen doc, existing_ids has now the ids to delete
        logger.debug(f"Delete {len(self.existing_ids)} docs from Elasticsearch")
        for doc_id in self.existing_ids.keys():
            await self.queue.put(
                {
                    "_op_type": OP_DELETE,
                    "_index": self.index,
                    "_id": doc_id,
                }
            )
            self.total_docs_deleted += 1

        await self.queue.put("END_DOCS")


class IndexMissing(Exception):
    pass


class ContentIndexNameInvalid(Exception):
    pass


class AsyncBulkRunningError(Exception):
    pass


class ElasticServer(ESClient):
    """This class is the sync orchestrator.

    It does the following in `async_bulk`

    - grabs all ids on Elasticsearch for the index
    - creates a MemQueue to hold documents to stream
    - runs a `Fetcher` (producer) and a `Bulker` (consumer) against the queue
    - once they are both over, returns totals
    """

    def __init__(self, elastic_config):
        logger.debug(f"ElasticServer connecting to {elastic_config['host']}")
        super().__init__(elastic_config)
        self.loop = asyncio.get_event_loop()
        self._fetcher = None
        self._fetcher_task = None
        self._bulker = None
        self._bulker_task = None

    async def prepare_content_index(self, index, *, mappings=None):
        """Creates the index, given a mapping if it does not exists."""
        if not index.startswith("search-"):
            raise ContentIndexNameInvalid(
                'Index name {index} is invalid. Index name must start with "search-"'
            )

        logger.debug(f"Checking index {index}")

        expand_wildcards = "open"
        exists = await self.client.indices.exists(
            index=index, expand_wildcards=expand_wildcards
        )
        if exists:
            logger.debug(f"{index} exists")
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
                    properties=mappings.get("properties", {}),
                    expand_wildcards=expand_wildcards,
                )
                logger.debug("Index %s mappings added", index)
            else:
                logger.debug("Index %s already has mappings. Skipping...", index)
            return
        else:
            raise IndexMissing(f"Index {index} does not exist!")

    async def get_existing_ids(self, index):
        """Returns an iterator on the `id` and `_timestamp` fields of all documents in an index.


        WARNING

        This function will load all ids in memory -- on very large indices,
        depending on the id length, it can be quite large.

        300,000 ids will be around 50MiB
        """
        logger.debug(f"Scanning existing index {index}")
        try:
            await self.client.indices.get(index=index)
        except ElasticNotFoundError:
            return

        async for doc in async_scan(
            client=self.client,
            index=index,
            _source=["id", TIMESTAMP_FIELD],
        ):
            doc_id = doc["_source"].get("id", doc["_id"])
            ts = doc["_source"].get(TIMESTAMP_FIELD)
            yield doc_id, ts

    def done(self):
        if self._fetcher_task is not None and not self._fetcher_task.done():
            return False
        if self._bulker_task is not None and not self._bulker_task.done():
            return False
        return True

    async def cancel(self):
        if self._fetcher_task is not None and not self._fetcher_task.done():
            self._fetcher_task.cancel()
            try:
                await self._fetcher_task
            except asyncio.CancelledError:
                logger.info("Fetcher is stopped.")
        if self._bulker_task is not None and not self._bulker_task.done():
            self._bulker_task.cancel()
            try:
                await self._bulker_task
            except asyncio.CancelledError:
                logger.info("Bulker is stopped.")

    def ingestion_stats(self):
        stats = {}
        if self._fetcher is not None:
            stats.update(
                {
                    "doc_created": self._fetcher.total_docs_created,
                    "attachment_extracted": self._fetcher.total_downloads,
                    "doc_updated": self._fetcher.total_docs_updated,
                    "doc_deleted": self._fetcher.total_docs_deleted,
                }
            )
        if self._bulker is not None:
            stats.update(
                {
                    "bulk_operations": dict(self._bulker.ops),
                    "indexed_document_count": self._bulker.indexed_document_count,
                    "indexed_document_volume": round(
                        self._bulker.indexed_document_volume
                    ),
                    "deleted_document_count": self._bulker.deleted_document_count,
                }
            )
        return stats

    def fetch_error(self):
        return None if self._fetcher is None else self._fetcher.fetch_error

    async def async_bulk(
        self,
        index,
        generator,
        pipeline,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        options=None,
    ):
        """Performs a batch of `_bulk` calls, given a generator of documents

        Arguments:
        - index: target index
        - generator: documents generator
        - pipeline: ingest pipeline settings to pass to the bulk API
        - filter_: an instance of `Filter` to apply on the fetched document  -- default: `None`
        - sync_rules_enabled: if enabled, applies rules -- default: `False`
        - content_extraction_enabled: if enabled, will download content -- default: `True`
        - options: dict of options (from `elasticsearch.bulk` in the config file)
        """
        if self._fetcher_task is not None or self._bulker_task is not None:
            raise AsyncBulkRunningError("Async bulk task has already started.")
        if filter_ is None:
            filter_ = Filter()
        if options is None:
            options = {}
        queue_size = options.get("queue_max_size", DEFAULT_QUEUE_SIZE)
        display_every = options.get("display_every", DEFAULT_DISPLAY_EVERY)
        queue_mem_size = options.get("queue_max_mem_size", DEFAULT_QUEUE_MEM_SIZE)
        chunk_mem_size = options.get("chunk_max_mem_size", DEFAULT_CHUNK_MEM_SIZE)
        max_concurrency = options.get("max_concurrency", DEFAULT_MAX_CONCURRENCY)
        chunk_size = options.get("chunk_size", DEFAULT_CHUNK_SIZE)
        concurrent_downloads = options.get(
            "concurrent_downloads", DEFAULT_CONCURRENT_DOWNLOADS
        )

        start = time.time()
        stream = MemQueue(maxsize=queue_size, maxmemsize=queue_mem_size * 1024 * 1024)
        existing_ids = {k: v async for (k, v) in self.get_existing_ids(index)}
        logger.debug(
            f"Found {len(existing_ids)} docs in {index} (duration "
            f"{int(time.time() - start)} seconds) "
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Size of ids in memory is {get_mb_size(existing_ids)}MiB")

        # start the fetcher
        self._fetcher = Fetcher(
            stream,
            index,
            existing_ids,
            filter_=filter_,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
            display_every=display_every,
            concurrent_downloads=concurrent_downloads,
        )
        self._fetcher_task = asyncio.create_task(self._fetcher.run(generator))

        # start the bulker
        self._bulker = Bulker(
            self.client,
            stream,
            chunk_size,
            pipeline,
            chunk_mem_size=chunk_mem_size,
            max_concurrency=max_concurrency,
        )
        self._bulker_task = asyncio.create_task(self._bulker.run())
