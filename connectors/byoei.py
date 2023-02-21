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

    The bulk requests are controlled in several ways:
    - `chunk_size` -- a maximum number of operations to send per request
    - `chunk_mem_size` -- a maximum size in MiB for each bulk request
    - `max_concurrency` -- a maximum number of concurrent bulk requests

    Extra options:
    - `pipeline` -- ingest pipeline settings to pass to the bulk API
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

    def _bulk_op(self, doc, operation=OP_INDEX):
        doc_id = doc["_id"]
        index = doc["_index"]

        if operation == OP_INDEX:
            return [{operation: {"_index": index, "_id": doc_id}}, doc["doc"]]
        if operation == OP_UPSERT:
            return [
                {"update": {"_index": index, "_id": doc_id}},
                {"doc": doc["doc"], "doc_as_upsert": True},
            ]
        if operation == OP_DELETE:
            return [{operation: {"_index": index, "_id": doc_id}}]

        raise TypeError(operation)

    async def _batch_bulk(self, operations):
        # todo treat result to retry errors like in async_streaming_bulk
        task_num = len(self.bulk_tasks)

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
        finally:
            self.bulk_time += time.time() - start

        return res

    async def run(self):
        batch = []
        self.bulk_time = 0
        self.bulking = True
        bulk_size = 0

        while True:
            doc_size, doc = await self.queue.get()
            if doc in ("END_DOCS", "FETCH_ERROR"):
                break
            operation = doc["_op_type"]
            self.ops[operation] += 1
            batch.extend(self._bulk_op(doc, operation))

            bulk_size += doc_size
            if len(batch) >= self.chunk_size or bulk_size > self.chunk_mem_size:
                await self.bulk_tasks.put(
                    functools.partial(
                        self._batch_bulk,
                        copy.copy(batch),
                    )
                )
                batch.clear()
                bulk_size = 0

            await asyncio.sleep(0)

        await self.bulk_tasks.join()
        if len(batch) > 0:
            await self._batch_bulk(batch)


class Fetcher:
    """Grabs data and adds them in the queue for the bulker.

    This class runs a coroutine that puts docs in `queue`, given
    a document generator.
    """

    def __init__(
        self,
        client,
        queue,
        index,
        existing_ids,
        filter_=None,
        sync_rules_enabled=False,
        queue_size=DEFAULT_QUEUE_SIZE,
        display_every=DEFAULT_DISPLAY_EVERY,
        concurrent_downloads=DEFAULT_CONCURRENT_DOWNLOADS,
    ):
        if filter_ is None:
            filter_ = Filter()
        self.client = client
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
        self.display_every = display_every
        self.concurrent_downloads = concurrent_downloads

    def __str__(self):
        return (
            "Fetcher <"
            f"create: {self.total_docs_created} |"
            f"update: {self.total_docs_updated} |"
            f"delete: {self.total_docs_deleted}>"
        )

    async def run(self, generator):
        await self.get_docs(generator)

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

    async def get_docs(self, generator):
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
                        if lazy_download is not None:
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
                if lazy_download is not None:
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

    async def async_bulk(
        self,
        index,
        generator,
        pipeline,
        filter_=None,
        sync_rules_enabled=False,
        options=None,
    ):
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
        logger.debug(f"Size of ids in memory is {get_mb_size(existing_ids)}MiB")

        # start the fetcher
        fetcher = Fetcher(
            self.client,
            stream,
            index,
            existing_ids,
            filter_=filter_,
            sync_rules_enabled=sync_rules_enabled,
            queue_size=queue_size,
            display_every=display_every,
            concurrent_downloads=concurrent_downloads,
        )
        fetcher_task = asyncio.create_task(fetcher.run(generator))

        # start the bulker
        bulker = Bulker(
            self.client,
            stream,
            chunk_size,
            pipeline,
            chunk_mem_size=chunk_mem_size,
            max_concurrency=max_concurrency,
        )
        bulker_task = asyncio.create_task(bulker.run())

        await asyncio.gather(fetcher_task, bulker_task)

        # we return a number for each operation type
        return {
            "bulk_operations": dict(bulker.ops),
            "doc_created": fetcher.total_docs_created,
            "attachment_extracted": fetcher.total_downloads,
            "doc_updated": fetcher.total_docs_updated,
            "doc_deleted": fetcher.total_docs_deleted,
            "fetch_error": fetcher.fetch_error,
        }
