#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Implementation of BYOEI protocol (+some ids collecting)
"""
import logging
import time
from collections import defaultdict
import asyncio

from elasticsearch import AsyncElasticsearch, NotFoundError as ElasticNotFoundError
from elasticsearch.helpers import async_scan

from connectors.logger import logger, set_extra_logger
from connectors.utils import iso_utc


class Bulker:
    """Send bulk operations in batches by consuming a queue."""

    def __init__(self, client, queue):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.ops = defaultdict(int)
        self.chunk_size = 500

    async def _batch_bulk(self, operations):
        # todo treat result to retry errors like in async_streaming_bulk
        start = time.time()
        try:
            res = await self.client.bulk(operations=operations)
        finally:
            self.bulk_time += time.time() - start

        logger.info(dict(self.ops))
        return res

    async def run(self):
        batch = []
        ops = []
        self.bulk_time = 0
        self.bulking = True
        docs_ended = downloads_ended = False
        while True:
            doc = await self.queue.get()
            if doc == "END_DOCS":
                docs_ended = True
            if doc == "END_DOWNLOADS":
                downloads_ended = True

            if docs_ended and downloads_ended:
                break

            if doc in ("END_DOCS", "END_DOWNLOADS"):
                continue

            operation = doc["_op_type"]
            self.ops[operation] += 1
            if operation in ("update", "create"):
                batch.append({"update": {"_index": doc["_index"], "_id": doc["_id"]}})
                batch.append({"doc": doc["doc"], "doc_as_upsert": True})
            else:
                batch.append({operation: {"_index": doc["_index"], "_id": doc["_id"]}})

            if len(batch) >= self.chunk_size * 2:
                ops.append(asyncio.create_task(self._batch_bulk(list(batch))))
                batch.clear()

        if len(batch) > 0:
            ops.append(asyncio.create_task(self._batch_bulk(list(batch))))
            batch.clear()

        await asyncio.gather(*ops)


class Fetcher:
    """Grab data and add them in the queue for the bulker"""

    def __init__(self, client, queue, index, existing_ids, existing_timestamps):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.index = index
        self._downloads = asyncio.Queue()
        self.loop = asyncio.get_event_loop()
        self.existing_ids = existing_ids
        self.existing_timestamps = existing_timestamps
        self.sync_runs = False

    # XXX this can be defferred
    async def get_attachments(self):
        downloads = 0
        logger.info("Starting downloads")

        while self.sync_runs:
            coro = await self._downloads.get()
            if coro == "END":
                break

            data = await coro
            await asyncio.sleep(0)

            if data is None:
                continue

            downloads += 1
            await self.queue.put(
                {
                    "_op_type": "update",
                    "_index": self.index,
                    "_id": data.pop("_id"),
                    "doc": data,
                    "doc_as_upsert": True,
                }
            )

            if divmod(downloads, 10)[-1] == 0:
                logger.info(f"Downloaded {downloads} files.")

            await asyncio.sleep(0)

        await self.queue.put("END_DOWNLOADS")
        logger.info(f"Downloads done {downloads} files.")

    async def run(self, generator):
        t1 = self.loop.create_task(self.get_docs(generator))
        t2 = self.loop.create_task(self.get_attachments())
        await asyncio.gather(t1, t2)

    async def get_docs(self, generator):
        logger.info("Starting doc lookups")
        self.sync_runs = True

        seen_ids = set()
        async for doc in generator:
            doc, lazy_download = doc
            doc_id = doc["id"] = doc.pop("_id")
            logger.debug(f"Looking at {doc_id}")
            seen_ids.add(doc_id)

            # If the doc has a timestamp, we can use it to see if it has
            # been modified. This reduces the bulk size a *lot*
            #
            # Some backends do not know how to do this so it's optional.
            # For them we update the docs in any case.
            if "timestamp" in doc:
                if self.existing_timestamps.get(doc_id, "") == doc["timestamp"]:
                    logger.debug(f"Skipping {doc_id}")
                    await lazy_download(doit=False)
                    continue
            else:
                doc["timestamp"] = iso_utc()

            if lazy_download is not None:
                await self._downloads.put(
                    self.loop.create_task(
                        lazy_download(doit=True, timestamp=doc["timestamp"])
                    )
                )

            if doc_id in self.existing_ids:
                operation = "update"
            else:
                operation = "create"

            await self.queue.put(
                {
                    "_op_type": operation,
                    "_index": self.index,
                    "_id": doc_id,
                    "doc": doc,
                    "doc_as_upsert": True,
                }
            )
            await asyncio.sleep(0)

        # We delete any document that existed in Elasticsearch that was not
        # returned by the backend.
        for doc_id in self.existing_ids:
            if doc_id in seen_ids:
                continue
            await self.queue.put(
                {"_op_type": "delete", "_index": self.index, "_id": doc_id}
            )

        await self._downloads.put("END")
        await self.queue.put("END_DOCS")


class ElasticServer:
    def __init__(self, elastic_config):
        logger.debug(f"ElasticServer connecting to {elastic_config['host']}")
        self.host = elastic_config["host"]
        self.auth = elastic_config["user"], elastic_config["password"]
        self.client = AsyncElasticsearch(
            hosts=[self.host], basic_auth=self.auth, request_timeout=120
        )
        self._downloads = []
        self.loop = asyncio.get_event_loop()
        level = elastic_config.get("log_level", "INFO")
        es_logger = logging.getLogger("elastic_transport.node")
        set_extra_logger(es_logger, log_level=logging.getLevelName(level))

    async def ping(self):
        await self.client.ping()

    async def close(self):
        await self.client.close()

    async def prepare_index(self, index, docs=None, mapping=None, delete_first=False):
        """Creates the index, given a mapping if it does not exists."""
        # XXX todo update the existing index with the new mapping
        logger.debug(f"Checking index {index}")
        exists = await self.client.indices.exists(
            index=index, expand_wildcards="hidden"
        )
        if exists:
            logger.debug(f"{index} exists")
            if not delete_first:
                return
            logger.debug("Deleting it first")
            await self.client.indices.delete(index=index, expand_wildcards="hidden")

        logger.debug(f"Creating index {index}")
        await self.client.indices.create(index=index)
        if docs is None:
            return
        # XXX bulk
        doc_id = 1
        for doc in docs:
            await self.client.index(index=index, id=doc_id, document=doc)
            doc_id += 1

    async def get_existing_ids(self, index):
        """Returns an iterator on the `id` and `timestamp` fields of all documents in an index."""

        logger.debug(f"Scanning existing index {index}")
        try:
            await self.client.indices.get(index=index)
        except ElasticNotFoundError:
            return

        async for doc in async_scan(
            client=self.client,
            index=index,
            _source=["id", "timestamp"],
        ):
            yield doc["_source"]

    async def async_bulk(self, index, generator):
        start = time.time()
        existing_ids = set()
        existing_timestamps = {}
        stream = asyncio.Queue()

        async for es_doc in self.get_existing_ids(index):
            existing_ids.add(es_doc["id"])
            existing_timestamps[es_doc["id"]] = es_doc["timestamp"]

        logger.debug(
            f"Found {len(existing_ids)} docs in {index} (duration "
            f"{int(time.time() - start)} seconds)"
        )

        # start the fetcher
        fetcher = Fetcher(self.client, stream, index, existing_ids, existing_timestamps)
        fetcher_task = asyncio.create_task(fetcher.run(generator))

        # start the bulker
        bulker = Bulker(self.client, stream)
        bulker_task = asyncio.create_task(bulker.run())

        await asyncio.gather(fetcher_task, bulker_task)

        # we return a number for each operation type.
        return dict(bulker.ops)
