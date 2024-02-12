#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
`SyncOrchestrator` is orchestrating a sync by:

- creating a queue
- launching a `Extractor`, a wrapper on the top of the documents' generator
- launching a `Sink`, a class that aggregates documents and run the bulk API


                  SyncOrchestrator.async_bulk(generator)
                               |
                               |
Elasticsearch <== Sink <== queue <== Extractor <== generator

"""
import asyncio
import copy
import functools
import logging
import time
from collections import defaultdict

from elasticsearch import (
    NotFoundError as ElasticNotFoundError,
)

from connectors.config import (
    DEFAULT_ELASTICSEARCH_MAX_RETRIES,
    DEFAULT_ELASTICSEARCH_RETRY_INTERVAL,
)
from connectors.es.management_client import ESManagementClient
from connectors.es.settings import TIMESTAMP_FIELD, Mappings
from connectors.filtering.basic_rule import BasicRuleEngine, parse
from connectors.logger import logger, tracer
from connectors.protocol import Filter, JobType
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
    aenumerate,
    get_size,
    iso_utc,
    retryable,
)

__all__ = ["SyncOrchestrator"]

OP_INDEX = "index"
OP_UPSERT = "update"
OP_DELETE = "delete"
CANCELATION_TIMEOUT = 5


def get_mb_size(ob):
    """Returns the size of ob in MiB"""
    return round(get_size(ob) / (1024 * 1024), 2)


class UnsupportedJobType(Exception):
    pass


class ForceCanceledError(Exception):
    pass


class ContentIndexDoesNotExistError(Exception):
    pass


class ApiKeyNotFoundError(Exception):
    pass


class Sink:
    """Send bulk operations in batches by consuming a queue.

    This class runs a coroutine that gets operations out of a `queue` and collects them to
    build and send bulk requests using a `client`

    Arguments:

    - `client` -- an instance of `connectors.es.ESManagementClient`
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
        max_retries,
        retry_interval,
        logger_=None,
    ):
        self.client = client
        self.queue = queue
        self.ops = defaultdict(int)
        self.chunk_size = chunk_size
        self.pipeline = pipeline
        self.chunk_mem_size = chunk_mem_size * 1024 * 1024
        self.bulk_tasks = ConcurrentTasks(max_concurrency=max_concurrency)
        self.max_retires = max_retries
        self.retry_interval = retry_interval
        self.indexed_document_count = 0
        self.indexed_document_volume = 0
        self.deleted_document_count = 0
        self._logger = logger_ or logger
        self._canceled = False

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

    @tracer.start_as_current_span("_bulk API call", slow_log=1.0)
    async def _batch_bulk(self, operations, stats):
        # TODO: make this retry policy work with unified retry strategy
        @retryable(retries=self.max_retires, interval=self.retry_interval)
        async def _bulk_api_call():
            return await self.client.client.bulk(
                operations=operations, pipeline=self.pipeline["name"]
            )

        # TODO: treat result to retry errors like in async_streaming_bulk
        task_num = len(self.bulk_tasks)

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Task {task_num} - Sending a batch of {len(operations)} ops -- {get_mb_size(operations)}MiB"
            )

        # TODO: retry 429s for individual items here
        res = await self.client.bulk_insert(operations, self.pipeline["name"])
        if res.get("errors"):
            for item in res["items"]:
                for op, data in item.items():
                    if "error" in data:
                        self._logger.error(f"operation {op} failed, {data['error']}")
                        raise Exception(data["error"]["reason"])

        self._populate_stats(stats, res)

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

        self._logger.debug(
            f"Sink stats - no. of docs indexed: {self.indexed_document_count}, volume of docs indexed: {round(self.indexed_document_volume)} bytes, no. of docs deleted: {self.deleted_document_count}"
        )

    def force_cancel(self):
        self._canceled = True

    async def fetch_doc(self):
        if self._canceled:
            raise ForceCanceledError

        return await self.queue.get()

    async def run(self):
        try:
            await self._run()
        except asyncio.CancelledError:
            self._logger.info("Task is canceled, stop Sink...")
            raise
        except Exception as e:
            if isinstance(e, ForceCanceledError) or self._canceled:
                self._logger.warning(
                    f"Sink did not stop within {CANCELATION_TIMEOUT} seconds of cancelation, force-canceling the task."
                )
                return
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
        bulk_size = 0
        overhead_size = None

        while True:
            doc_size, doc = await self.fetch_doc()
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


class Extractor:
    """Grabs data and adds them in the queue for the bulker.

    This class runs a coroutine that puts docs in `queue`, given a document generator.

    Arguments:
    - client: an instance of `connectors.es.ESManagementClient`
    - queue: an `asyncio.Queue` to put docs in
    - index: the target Elasticsearch index
    - filter_: an instance of `Filter` to apply on the fetched document -- default: `None`
    - sync_rules_enabled: if `True`, we apply rules -- default: `False`
    - content_extraction_enabled: if `True`, download content -- default `True`
    - display_every -- display a log every `display_every` doc -- default: `DEFAULT_DISPLAY_EVERY`
    - concurrent_downloads: -- concurrency level for downloads -- default: `DEFAULT_CONCURRENT_DOWNLOADS`
    """

    def __init__(
        self,
        client,
        queue,
        index,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        display_every=DEFAULT_DISPLAY_EVERY,
        concurrent_downloads=DEFAULT_CONCURRENT_DOWNLOADS,
        logger_=None,
        skip_unchanged_documents=False,
    ):
        if filter_ is None:
            filter_ = Filter()
        self.client = client
        self.queue = queue
        self.index = index
        self.loop = asyncio.get_event_loop()
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
        self._logger = logger_ or logger
        self._canceled = False
        self.skip_unchanged_documents = skip_unchanged_documents

    async def _deferred_index(self, lazy_download, doc_id, doc, operation):
        data = await lazy_download(doit=True, timestamp=doc[TIMESTAMP_FIELD])

        if data is not None:
            self.total_downloads += 1
            data.pop("_id", None)
            data.pop(TIMESTAMP_FIELD, None)
            doc.update(data)

        doc.pop("_original_filename", None)

        await self.put_doc(
            {
                "_op_type": operation,
                "_index": self.index,
                "_id": doc_id,
                "doc": doc,
            }
        )

    def force_cancel(self):
        self._canceled = True

    async def put_doc(self, doc):
        if self._canceled:
            raise ForceCanceledError

        await self.queue.put(doc)

    async def run(self, generator, job_type):
        try:
            match job_type:
                case JobType.FULL:
                    await self.get_docs(generator)
                case JobType.INCREMENTAL:
                    if self.skip_unchanged_documents:
                        await self.get_docs(generator, skip_unchanged_documents=True)
                    else:
                        await self.get_docs_incrementally(generator)
                case JobType.ACCESS_CONTROL:
                    await self.get_access_control_docs(generator)
                case _:
                    raise UnsupportedJobType
        except asyncio.CancelledError:
            self._logger.info("Task is canceled, stop Extractor...")
            raise
        except Exception as e:
            if isinstance(e, ForceCanceledError) or self._canceled:
                self._logger.warning(
                    f"Extractor did not stop within {CANCELATION_TIMEOUT} seconds of cancelation, force-canceling the task."
                )
                return

            self._logger.critical("Document extractor failed", exc_info=True)
            await self.put_doc("FETCH_ERROR")
            self.fetch_error = e

    @tracer.start_as_current_span("get_doc call", slow_log=1.0)
    async def _decorate_with_metrics_span(self, generator):
        """Wrapper for metrics"""
        async for doc in generator:
            yield doc

    async def get_docs(self, generator, skip_unchanged_documents=False):
        """Iterate on a generator of documents to fill a queue of bulk operations for the `Sink` to consume.
        Extraction happens in a separate task, when a document contains files.

        Args:
           generator (generator): BaseDataSource child get_docs or get_docs_incrementally
           skip_unchanged_documents (bool): if True, will skip documents that have not changed since last sync
        """
        generator = self._decorate_with_metrics_span(generator)
        existing_ids = await self._load_existing_docs()

        self._logger.info("Iterating on remote documents")
        lazy_downloads = ConcurrentTasks(self.concurrent_downloads)
        try:
            async for count, doc in aenumerate(generator):
                doc, lazy_download, operation = doc
                if count % self.display_every == 0:
                    self._log_progress()

                doc_id = doc["id"] = doc.pop("_id")

                if self.basic_rule_engine and not self.basic_rule_engine.should_ingest(
                    doc
                ):
                    continue

                if doc_id in existing_ids:
                    # pop out of existing_ids, so they do not get deleted
                    ts = existing_ids.pop(doc_id)

                    if (
                        skip_unchanged_documents
                        and TIMESTAMP_FIELD in doc
                        and ts == doc[TIMESTAMP_FIELD]
                    ):
                        # cancel the download
                        if (
                            self.content_extraction_enabled
                            and lazy_download is not None
                        ):
                            await lazy_download(doit=False)

                        self._logger.debug(
                            f"Skipping document with id '{doc_id}' because field '{TIMESTAMP_FIELD}' has not changed since last sync"
                        )
                        continue

                    self.total_docs_updated += 1
                else:
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
                    await self.put_doc(
                        {
                            "_op_type": operation,
                            "_index": self.index,
                            "_id": doc_id,
                            "doc": doc,
                        }
                    )

                await asyncio.sleep(0)
        finally:
            # wait for all downloads to be finished
            await lazy_downloads.join()

        await self.enqueue_docs_to_delete(existing_ids)
        await self.put_doc("END_DOCS")

    async def _load_existing_docs(self):
        start = time.time()
        self._logger.info("Collecting local document ids")

        existing_ids = {
            k: v
            async for (k, v) in self.client.yield_existing_documents_metadata(
                self.index
            )
        }

        self._logger.debug(
            f"Found {len(existing_ids)} docs in {self.index} (duration "
            f"{int(time.time() - start)} seconds) "
        )

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Size of ids in memory is {get_mb_size(existing_ids)}MiB"
            )

        return existing_ids

    async def get_docs_incrementally(self, generator):
        """Iterate on a generator of documents to fill a queue of bulk operations for the `Sink` to consume.

        A document might be discarded if its timestamp has not changed.
        Extraction happens in a separate task, when a document contains files.
        """
        generator = self._decorate_with_metrics_span(generator)

        self._logger.info("Iterating on remote documents incrementally when possible")
        lazy_downloads = ConcurrentTasks(self.concurrent_downloads)
        try:
            async for count, doc in aenumerate(generator):
                doc, lazy_download, operation = doc
                if count % self.display_every == 0:
                    self._log_progress()

                doc_id = doc["id"] = doc.pop("_id")

                if self.basic_rule_engine and not self.basic_rule_engine.should_ingest(
                    doc
                ):
                    continue

                if operation == OP_INDEX:
                    self.total_docs_created += 1
                elif operation == OP_UPSERT:
                    self.total_docs_updated += 1
                elif operation == OP_DELETE:
                    self.total_docs_deleted += 1
                else:
                    self._logger.error(
                        f"unsupported operation {operation} for doc {doc_id}"
                    )

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
                    item = {
                        "_op_type": operation,
                        "_index": self.index,
                        "_id": doc_id,
                    }
                    if operation in (OP_INDEX, OP_UPSERT):
                        item["doc"] = doc
                    await self.put_doc(item)

                await asyncio.sleep(0)
        finally:
            # wait for all downloads to be finished
            await lazy_downloads.join()

        await self.put_doc("END_DOCS")

    async def get_access_control_docs(self, generator):
        """Iterate on a generator of access control documents to fill a queue with bulk operations for the `Sink` to consume.

        A document might be discarded if its timestamp has not changed.
        """
        self._logger.info("Starting access control doc lookups")
        generator = self._decorate_with_metrics_span(generator)

        existing_ids = {
            doc_id: last_update_timestamp
            async for (
                doc_id,
                last_update_timestamp,
            ) in self.client.yield_existing_documents_metadata(self.index)
        }

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(
                f"Size of {len(existing_ids)} access control document ids  in memory is {get_mb_size(existing_ids)}MiB"
            )

        count = 0
        async for doc in generator:
            doc, _, _ = doc
            count += 1
            if count % self.display_every == 0:
                self._log_progress()

            doc_id = doc["id"] = doc.pop("_id")
            doc_exists = doc_id in existing_ids

            if doc_exists:
                last_update_timestamp = existing_ids.pop(doc_id)
                doc_not_updated = (
                    TIMESTAMP_FIELD in doc
                    and last_update_timestamp == doc[TIMESTAMP_FIELD]
                )

                if doc_not_updated:
                    continue

                self.total_docs_updated += 1

                operation = OP_UPSERT
            else:
                self.total_docs_created += 1

                if TIMESTAMP_FIELD not in doc:
                    doc[TIMESTAMP_FIELD] = iso_utc()

                operation = OP_INDEX

            await self.put_doc(
                {
                    "_op_type": operation,
                    "_index": self.index,
                    "_id": doc_id,
                    "doc": doc,
                }
            )
            await asyncio.sleep(0)

        await self.enqueue_docs_to_delete(existing_ids)
        await self.put_doc("END_DOCS")

    async def enqueue_docs_to_delete(self, existing_ids):
        self._logger.debug(f"Delete {len(existing_ids)} docs from index '{self.index}'")
        for doc_id in existing_ids.keys():
            await self.put_doc(
                {
                    "_op_type": OP_DELETE,
                    "_index": self.index,
                    "_id": doc_id,
                }
            )
            self.total_docs_deleted += 1

    def _log_progress(self):
        self._logger.info(
            "Sync progress -- "
            f"created: {self.total_docs_created} | "
            f"updated: {self.total_docs_updated} | "
            f"deleted: {self.total_docs_deleted}"
        )


class ContentIndexNameInvalid(Exception):
    pass


class AsyncBulkRunningError(Exception):
    pass


class SyncOrchestrator:
    """This class is the sync orchestrator.

    It does the following in `async_bulk`

    - grabs all ids on Elasticsearch for the index
    - creates a MemQueue to hold documents to stream
    - runs a `Extractor` (producer) and a `Sink` (consumer) against the queue
    - once they are both over, returns totals
    """

    def __init__(self, elastic_config, logger_=None):
        self._logger = logger_ or logger
        self._logger.debug(f"SyncOrchestrator connecting to {elastic_config['host']}")
        self.es_management_client = ESManagementClient(elastic_config)
        self.loop = asyncio.get_event_loop()
        self._extractor = None
        self._extractor_task = None
        self._sink = None
        self._sink_task = None

    async def close(self):
        await self.es_management_client.close()

    async def update_authorization(self, index_name, secret_id):
        # Updates the ESManagementClient auth options for native connectors after fetching API key
        try:
            api_key = await self.es_management_client.get_connector_secret(secret_id)
            self._logger.debug(
                f"Using API key found in secrets storage for authorization for index [{index_name}]."
            )
            self.es_management_client.client.options(api_key=api_key)
        except ElasticNotFoundError as e:
            msg = f"API key not found in secrets storage for index [{index_name}]."
            raise ApiKeyNotFoundError(msg) from e

    async def has_active_license_enabled(self, license_):
        # TODO: think how to make it not a proxy method to the client
        return await self.es_management_client.has_active_license_enabled(license_)

    async def prepare_content_index(self, index, language_code=None):
        """Creates the index, given a mapping if it does not exists."""
        self._logger.debug(f"Checking index {index}")

        exists = await self.es_management_client.index_exists(index)

        mappings = Mappings.default_text_fields_mappings(is_connectors_index=True)

        if exists:
            # Update the index mappings if needed
            self._logger.debug(f"{index} exists")
            await self.es_management_client.ensure_content_index_mappings(
                index, mappings
            )
        else:
            # Create a new index
            self._logger.info(f"Creating content index: {index}")
            await self.es_management_client.create_content_index(index, language_code)
            self._logger.info(f"Content index successfully created:  {index}")

    def done(self):
        if self._extractor_task is not None and not self._extractor_task.done():
            return False
        if self._sink_task is not None and not self._sink_task.done():
            return False
        return True

    def _sink_task_running(self):
        return self._sink_task is not None and not self._sink_task.done()

    def _extractor_task_running(self):
        return self._extractor_task is not None and not self._extractor_task.done()

    async def cancel(self):
        if self._sink_task_running():
            self._sink_task.cancel()
        if self._extractor_task_running():
            self._extractor_task.cancel()

        cancelation_timeout = CANCELATION_TIMEOUT
        while cancelation_timeout > 0:
            await asyncio.sleep(1)
            cancelation_timeout -= 1
            if not self._sink_task_running() and not self._extractor_task_running():
                self._logger.info(
                    "Both Extractor and Sink tasks are successfully stopped."
                )
                return

        self._logger.error(
            f"Sync job did not stop within {CANCELATION_TIMEOUT} seconds of canceling. Force-canceling."
        )
        self._sink.force_cancel()
        self._extractor.force_cancel()

    def ingestion_stats(self):
        stats = {}
        if self._extractor is not None:
            stats.update(
                {
                    "doc_created": self._extractor.total_docs_created,
                    "attachment_extracted": self._extractor.total_downloads,
                    "doc_updated": self._extractor.total_docs_updated,
                    "doc_deleted": self._extractor.total_docs_deleted,
                }
            )
        if self._sink is not None:
            stats.update(
                {
                    "bulk_operations": dict(self._sink.ops),
                    "indexed_document_count": self._sink.indexed_document_count,
                    # return indexed_document_volume in number of MiB
                    "indexed_document_volume": round(
                        self._sink.indexed_document_volume / (1024 * 1024)
                    ),
                    "deleted_document_count": self._sink.deleted_document_count,
                }
            )
        return stats

    def fetch_error(self):
        return None if self._extractor is None else self._extractor.fetch_error

    async def async_bulk(
        self,
        index,
        generator,
        pipeline,
        job_type,
        filter_=None,
        sync_rules_enabled=False,
        content_extraction_enabled=True,
        options=None,
        skip_unchanged_documents=False,
    ):
        """Performs a batch of `_bulk` calls, given a generator of documents

        Arguments:
        - index: target index
        - generator: documents generator
        - pipeline: ingest pipeline settings to pass to the bulk API
        - job_type: the job type of the sync job
        - filter_: an instance of `Filter` to apply on the fetched document  -- default: `None`
        - sync_rules_enabled: if enabled, applies rules -- default: `False`
        - content_extraction_enabled: if enabled, will download content -- default: `True`
        - options: dict of options (from `elasticsearch.bulk` in the config file)
        """
        if self._extractor_task is not None or self._sink_task is not None:
            msg = "Async bulk task has already started."
            raise AsyncBulkRunningError(msg)
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
        max_bulk_retries = options.get("max_retries", DEFAULT_ELASTICSEARCH_MAX_RETRIES)
        retry_interval = options.get(
            "retry_interval", DEFAULT_ELASTICSEARCH_RETRY_INTERVAL
        )
        mem_queue_refresh_timeout = options.get("queue_refresh_timeout", 60)
        mem_queue_refresh_interval = options.get("queue_refresh_interval", 1)

        stream = MemQueue(
            maxsize=queue_size,
            maxmemsize=queue_mem_size * 1024 * 1024,
            refresh_timeout=mem_queue_refresh_timeout,
            refresh_interval=mem_queue_refresh_interval,
        )

        # start the fetcher
        self._extractor = Extractor(
            self.es_management_client,
            stream,
            index,
            filter_=filter_,
            sync_rules_enabled=sync_rules_enabled,
            content_extraction_enabled=content_extraction_enabled,
            display_every=display_every,
            concurrent_downloads=concurrent_downloads,
            logger_=self._logger,
            skip_unchanged_documents=skip_unchanged_documents,
        )
        self._extractor_task = asyncio.create_task(
            self._extractor.run(generator, job_type)
        )

        # start the bulker
        self._sink = Sink(
            self.es_management_client,
            stream,
            chunk_size,
            pipeline,
            chunk_mem_size=chunk_mem_size,
            max_concurrency=max_concurrency,
            max_retries=max_bulk_retries,
            retry_interval=retry_interval,
            logger_=self._logger,
        )
        self._sink_task = asyncio.create_task(self._sink.run())
