#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOEI protocol (+some ids collecting)
"""
import time
from collections import defaultdict
import asyncio

from elasticsearch import NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan

from connectors.logger import logger
from connectors.utils import (
    iso_utc,
    ESClient,
    get_size,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_QUEUE_SIZE,
    DEFAULT_QUEUE_MEM_SIZE,
    DEFAULT_CHUNK_MEM_SIZE,
    DEFAULT_DISPLAY_EVERY,
    MemQueue,
)


OP_INDEX = "index"
OP_UPSERT = "update"
OP_DELETE = "delete"
TIMESTAMP_FIELD = "_timestamp"


class Bulker:
    """Send bulk operations in batches by consuming a queue."""

    def __init__(self, client, queue, chunk_size, pipeline_settings, chunk_mem_size):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.ops = defaultdict(int)
        self.chunk_size = chunk_size
        self.pipeline_settings = pipeline_settings
        self.chunk_mem_size = chunk_mem_size

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
        logger.debug(
            f"Sending a batch of {len(operations)} ops -- {get_size(operations)}MiB"
        )
        start = time.time()
        try:
            res = await self.client.bulk(
                operations=operations, pipeline=self.pipeline_settings.name
            )
            if res.get("errors"):
                for item in res["items"]:
                    for op, data in item.items():
                        if "error" in data:
                            logger.error(f"operation {op} failed, {data['error']}")
                            raise Exception(data["error"]["reason"])
        finally:
            self.bulk_time += time.time() - start

        logger.info(dict(self.ops))
        return res

    async def run(self):
        batch = []
        self.bulk_time = 0
        self.bulking = True
        while True:
            doc = await self.queue.get()
            if doc in ("END_DOCS", "FETCH_ERROR"):
                break
            operation = doc["_op_type"]
            self.ops[operation] += 1
            batch.extend(self._bulk_op(doc, operation))

            if len(batch) >= self.chunk_size or get_size(batch) > self.chunk_mem_size:
                await self._batch_bulk(batch)
                batch.clear()

            await asyncio.sleep(0)

        if len(batch) > 0:
            await self._batch_bulk(batch)


class Fetcher:
    """Grab data and add them in the queue for the bulker"""

    def __init__(
        self,
        client,
        queue,
        index,
        existing_ids,
        queue_size=DEFAULT_QUEUE_SIZE,
        display_every=DEFAULT_DISPLAY_EVERY,
    ):
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
        self.display_every = display_every

    def __str__(self):
        return (
            "Fetcher <"
            f"create: {self.total_docs_created} |"
            f"update: {self.total_docs_updated} |"
            f"delete: {self.total_docs_deleted}>"
        )

    async def run(self, generator):
        await self.get_docs(generator)

    async def get_docs(self, generator):
        logger.info("Starting doc lookups")
        self.sync_runs = True
        count = 0
        try:
            async for doc in generator:
                doc, lazy_download = doc
                count += 1
                if count % self.display_every == 0:
                    logger.info(str(self))

                doc_id = doc["id"] = doc.pop("_id")

                if doc_id in self.existing_ids:
                    # pop out of self.existing_ids
                    ts = self.existing_ids.pop(doc_id)

                    # If the doc has a timestamp, we can use it to see if it has
                    # been modified. This reduces the bulk size a *lot*
                    #
                    # Some backends do not know how to do this so it's optional.
                    # For them we update the docs in any case.
                    if TIMESTAMP_FIELD in doc and ts == doc[TIMESTAMP_FIELD]:
                        # cancel the download
                        if lazy_download is not None:
                            await lazy_download(doit=False)
                        continue

                    # the doc exists but we are still overwiting it with `index`
                    operation = OP_INDEX
                    self.total_docs_updated += 1
                else:
                    operation = OP_INDEX
                    self.total_docs_created += 1
                    if TIMESTAMP_FIELD not in doc:
                        doc[TIMESTAMP_FIELD] = iso_utc()

                if lazy_download is not None:
                    data = await lazy_download(
                        doit=True, timestamp=doc[TIMESTAMP_FIELD]
                    )
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
                await asyncio.sleep(0)
        except Exception as e:
            logger.critical("The document fetcher failed", exc_info=True)
            await self.queue.put("FETCH_ERROR")
            self.fetch_error = e
            return

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


class ElasticServer(ESClient):
    def __init__(self, elastic_config):
        logger.debug(f"ElasticServer connecting to {elastic_config['host']}")
        super().__init__(elastic_config)
        self.loop = asyncio.get_event_loop()

    async def prepare_index(
        self, index, *, docs=None, settings=None, mappings=None, delete_first=False
    ):
        """Creates the index, given a mapping if it does not exists."""
        if index.startswith("."):
            expand_wildcards = "hidden"
        else:
            expand_wildcards = "open"

        logger.debug(f"Checking index {index}")
        exists = await self.client.indices.exists(
            index=index, expand_wildcards=expand_wildcards
        )
        if exists and delete_first:
            logger.debug(f"{index} exists, deleting...")
            logger.debug("Deleting it first")
            await self.client.indices.delete(
                index=index, expand_wildcards=expand_wildcards
            )
            exists = False
        if exists:
            logger.debug(f"{index} exists")
            if delete_first:
                logger.debug("Deleting it first")
                await self.client.indices.delete(
                    index=index, expand_wildcards=expand_wildcards
                )
                return
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

        logger.debug(f"Creating index {index}")
        await self.client.indices.create(
            index=index, settings=settings, mappings=mappings
        )
        if docs is None:
            return
        # XXX bulk
        doc_id = 1
        for doc in docs:
            await self.client.index(index=index, id=doc_id, document=doc)
            doc_id += 1

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
        queue_size=DEFAULT_QUEUE_SIZE,
        display_every=DEFAULT_DISPLAY_EVERY,
        queue_mem_size=DEFAULT_QUEUE_MEM_SIZE,
        chunk_mem_size=DEFAULT_CHUNK_MEM_SIZE,
    ):
        start = time.time()
        stream = MemQueue(maxsize=queue_size, maxmemsize=queue_mem_size * 1024 * 1024)
        existing_ids = {k: v async for (k, v) in self.get_existing_ids(index)}
        logger.debug(
            f"Found {len(existing_ids)} docs in {index} (duration "
            f"{int(time.time() - start)} seconds) "
        )
        logger.debug(f"Size of ids in memory is {get_size(existing_ids)}MiB")

        # start the fetcher
        fetcher = Fetcher(
            self.client,
            stream,
            index,
            existing_ids,
            queue_size=queue_size,
            display_every=display_every,
        )
        fetcher_task = asyncio.create_task(fetcher.run(generator))

        # start the bulker
        bulker = Bulker(
            self.client,
            stream,
            self.config.get("bulk_chunk_size", DEFAULT_CHUNK_SIZE),
            pipeline,
            chunk_mem_size=chunk_mem_size,
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
