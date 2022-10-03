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
from connectors.utils import iso_utc, ESClient


OP_INDEX = "index"
OP_UPSERT = "update"
OP_DELETE = "delete"


class Bulker:
    """Send bulk operations in batches by consuming a queue."""

    def __init__(self, client, queue, chunk_size, pipeline_settings):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.ops = defaultdict(int)
        self.chunk_size = chunk_size
        self.pipeline_settings = pipeline_settings

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
        ops = []
        self.bulk_time = 0
        self.bulking = True
        docs_ended = downloads_ended = False
        while True:
            doc = await self.queue.get()
            if doc in ("END_DOCS", "FETCH_ERROR"):
                docs_ended = True
            if doc == "END_DOWNLOADS":
                downloads_ended = True

            if docs_ended and downloads_ended:
                break

            if doc in ("END_DOCS", "END_DOWNLOADS", "FETCH_ERROR"):
                continue

            operation = doc["_op_type"]
            self.ops[operation] += 1
            batch.extend(self._bulk_op(doc, operation))

            if len(batch) >= self.chunk_size * 2:
                ops.append(asyncio.create_task(self._batch_bulk(list(batch))))
                batch.clear()

            await asyncio.sleep(0)

        if len(batch) > 0:
            ops.append(asyncio.create_task(self._batch_bulk(list(batch))))
            batch.clear()

        await asyncio.gather(*ops)


class Fetcher:
    """Grab data and add them in the queue for the bulker"""

    def __init__(
        self,
        client,
        queue,
        index,
        existing_ids,
        existing_timestamps,
        queue_size=1024,
    ):
        self.client = client
        self.queue = queue
        self.bulk_time = 0
        self.bulking = False
        self.index = index
        self._downloads = asyncio.Queue(maxsize=queue_size)
        self.loop = asyncio.get_event_loop()
        self.existing_ids = existing_ids
        self.existing_timestamps = existing_timestamps
        self.sync_runs = False
        self.total_downloads = 0
        self.total_docs_updated = 0
        self.total_docs_created = 0
        self.total_docs_deleted = 0
        self.fetch_error = None

    # XXX this can be defferred
    async def get_attachments(self):
        logger.info("Starting downloads")

        while self.sync_runs:
            coro = await self._downloads.get()
            if coro == "END":
                break

            data = await coro
            await asyncio.sleep(0)

            if data is None:
                continue

            self.total_downloads += 1

            # This is an upsert because we're pusing content
            # from attached file *after* the document was indexed
            await self.queue.put(
                {
                    "_op_type": OP_UPSERT,
                    "_index": self.index,
                    "_id": data.pop("_id"),
                    "doc": data,
                }
            )

            if divmod(self.total_downloads, 10)[-1] == 0:
                logger.info(f"Downloaded {self.total_downloads} files.")

            await asyncio.sleep(0)

        await self.queue.put("END_DOWNLOADS")
        logger.info(f"Downloads done {self.total_downloads} files.")

    async def run(self, generator):
        t1 = self.loop.create_task(self.get_docs(generator))
        t2 = self.loop.create_task(self.get_attachments())
        await asyncio.gather(t1, t2)

    async def get_docs(self, generator):
        logger.info("Starting doc lookups")
        self.sync_runs = True

        seen_ids = set()
        try:
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
                        # cancel the download
                        if lazy_download is not None:
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
                    # the doc exists but we are still overwiting it with `index`
                    operation = OP_INDEX
                    self.total_docs_updated += 1
                else:
                    operation = OP_INDEX
                    self.total_docs_created += 1

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
            await self._downloads.put("END")
            await self.queue.put("FETCH_ERROR")
            self.fetch_error = e
            return

        # We delete any document that existed in Elasticsearch that was not
        # returned by the backend.
        for doc_id in self.existing_ids:
            if doc_id in seen_ids:
                continue
            await self.queue.put(
                {"_op_type": OP_DELETE, "_index": self.index, "_id": doc_id}
            )
            self.total_docs_deleted += 1

        await self._downloads.put("END")
        await self.queue.put("END_DOCS")


class ElasticServer(ESClient):
    def __init__(self, elastic_config):
        logger.debug(f"ElasticServer connecting to {elastic_config['host']}")
        super().__init__(elastic_config)
        self._downloads = []
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
            if "id" not in doc["_source"]:
                doc["_source"]["id"] = doc["_id"]
            yield doc["_source"]

    async def async_bulk(self, index, generator, pipeline, queue_size=1024):
        start = time.time()
        existing_ids = set()
        existing_timestamps = {}
        stream = asyncio.Queue(maxsize=queue_size)

        async for es_doc in self.get_existing_ids(index):
            existing_ids.add(es_doc["id"])
            if "timestamp" in es_doc:
                existing_timestamps[es_doc["id"]] = es_doc["timestamp"]

        logger.debug(
            f"Found {len(existing_ids)} docs in {index} (duration "
            f"{int(time.time() - start)} seconds)"
        )

        # start the fetcher
        fetcher = Fetcher(
            self.client,
            stream,
            index,
            existing_ids,
            existing_timestamps,
            queue_size=queue_size,
        )
        fetcher_task = asyncio.create_task(fetcher.run(generator))

        # start the bulker
        bulker = Bulker(
            self.client, stream, self.config.get("bulk_chunk_size", 500), pipeline
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
