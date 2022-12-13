#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime, timezone
import base64
import logging
import time
import asyncio
import tracemalloc
import gc
import contextlib
import functools

from elasticsearch import (
    AsyncElasticsearch,
    ApiError,
    ConnectionError as ElasticConnectionError,
    NotFoundError,
)
from elastic_transport.client_utils import url_to_node_config
from guppy import hpy
from pympler import asizeof
from cstriggers.core.trigger import QuartzCron

from connectors.logger import set_extra_logger, logger


DEFAULT_CHUNK_SIZE = 500
DEFAULT_QUEUE_SIZE = 1024
DEFAULT_DISPLAY_EVERY = 100
DEFAULT_QUEUE_MEM_SIZE = 5
DEFAULT_CHUNK_MEM_SIZE = 25
DEFAULT_MAX_CONCURRENCY = 5
TIKA_SUPPORTED_FILETYPES = [
    ".txt",
    ".py",
    ".rst",
    ".html",
    ".markdown",
    ".json",
    ".xml",
    ".csv",
    ".md",
    ".ppt",
    ".rtf",
    ".docx",
    ".odt",
    ".xls",
    ".xlsx",
    ".rb",
    ".paper",
    ".sh",
    ".pptx",
    ".pdf",
    ".doc",
]


class ESClient:
    def __init__(self, config):
        self.config = config
        self.host = url_to_node_config(
            config.get("host", "http://localhost:9200"),
            use_default_ports_for_scheme=True,
        )
        self._sleeps = CancellableSleeps()
        options = {
            "hosts": [self.host],
            "request_timeout": config.get("request_timeout", 120),
            "retry_on_timeout": config.get("retry_on_timeout", True),
        }
        logger.debug(f"Host is {self.host}")

        if "username" in config:
            if "api_key" in config:
                raise KeyError("You can't use basic auth and Api Key at the same time")
            auth = config["username"], config["password"]
            options["basic_auth"] = auth
            logger.debug(
                f"Connecting using Basic Auth (user: {config['username']}, password: {config['password'][:3]}...)"
            )
        elif "api_key" in config:
            logger.debug(f"Connecting with an Api Key ({config['api_key'][:5]}...)")
            options["api_key"] = config["api_key"]

        if config.get("ssl", False):
            options["verify_certs"] = True
            if "ca_certs" in config:
                ca_certs = config["ca_certs"]
                logger.debug(f"Verifying cert with {ca_certs}")
                options["ca_certs"] = ca_certs

        level = config.get("log_level", "INFO").upper()
        es_logger = logging.getLogger("elastic_transport.node")
        set_extra_logger(
            es_logger, log_level=logging.getLevelName(level), filebeat=logger.filebeat
        )
        self.max_wait_duration = config.get("max_wait_duration", 60)
        self.initial_backoff_duration = config.get("initial_backoff_duration", 5)
        self.backoff_multiplier = config.get("backoff_multiplier", 2)
        self.client = AsyncElasticsearch(**options)
        self._keep_waiting = True

    def stop_waiting(self):
        self._keep_waiting = False
        self._sleeps.cancel()

    async def close(self):
        await self.client.close()

    async def ping(self):
        try:
            await self.client.info()
        except ApiError as e:
            logger.error(f"The server returned a {e.status_code} code")
            if e.info is not None and "error" in e.info and "reason" in e.info["error"]:
                logger.error(e.info["error"]["reason"])
            return False
        except ElasticConnectionError as e:
            logger.error("Could not connect to the server")
            if e.message is not None:
                logger.error(e.message)
            return False
        return True

    async def wait(self):
        backoff = self.initial_backoff_duration
        start = time.time()
        logger.debug(f"Wait for Elasticsearch (max: {self.max_wait_duration})")
        while time.time() - start < self.max_wait_duration:
            if not self._keep_waiting:
                await self.close()
                return False

            logger.info(
                f"Waiting for {self.host} (so far: {int(time.time() - start)} secs)"
            )
            if await self.ping():
                return True
            await self._sleeps.sleep(backoff)
            backoff *= self.backoff_multiplier

        await self.close()
        return False

    async def check_exists(self, indices=None, pipelines=None):
        if indices is None:
            indices = []
        if pipelines is None:
            pipelines = []

        for index in indices:
            logger.debug(f"Checking for index {index} presence")
            if not await self.client.indices.exists(index=index):
                raise PreflightCheckError(f"Cloud not find index {index}")

        for pipeline in pipelines:
            logger.debug(f"Checking for pipeline {pipeline} presence")
            try:
                await self.client.ingest.get_pipeline(id=pipeline)
            except NotFoundError:
                raise PreflightCheckError(f"Cloud not find pipeline {pipeline}")


def iso_utc(when=None):
    if when is None:
        when = datetime.now(timezone.utc)
    return when.isoformat()


def next_run(quartz_definition):
    """Returns the number of seconds before the next run."""
    cron_obj = QuartzCron(quartz_definition, datetime.utcnow())
    when = cron_obj.next_trigger()
    now = datetime.utcnow()
    secs = (when - now).total_seconds()
    if secs < 1.0:
        secs = 0
    return secs


INVALID_CHARS = "\\", "/", "*", "?", '"', "<", ">", "|", " ", ",", "#"
INVALID_PREFIX = "_", "-", "+"
INVALID_NAME = "..", "."


class InvalidIndexNameError(ValueError):
    pass


class PreflightCheckError(Exception):
    pass


def validate_index_name(name):
    for char in INVALID_CHARS:
        if char in name:
            raise InvalidIndexNameError(f"Invalid character {char}")

    if name.startswith(INVALID_PREFIX):
        raise InvalidIndexNameError(f"Invalid prefix {name[0]}")

    if not name.islower():
        raise InvalidIndexNameError("Must be lowercase")

    if name in INVALID_NAME:
        raise InvalidIndexNameError("Can't use that name")

    return name


class CancellableSleeps:
    def __init__(self):
        self._sleeps = set()

    async def sleep(self, delay, result=None, *, loop=None):
        async def _sleep(delay, result=None, *, loop=None):
            coro = asyncio.sleep(delay, result=result)
            task = asyncio.ensure_future(coro)
            self._sleeps.add(task)
            try:
                return await task
            except asyncio.CancelledError:
                logger.debug("Sleep canceled")
                return result
            finally:
                self._sleeps.remove(task)

        await _sleep(delay, result=result, loop=loop)

    def cancel(self):
        for task in self._sleeps:
            task.cancel()


def _snapshot():
    if not tracemalloc.is_tracing():
        tracemalloc.start()
    logger.info("Taking a memory snapshot")
    gc.collect()
    trace = tracemalloc.take_snapshot()
    return trace.filter_traces(
        (
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<frozen importlib._bootstrap_external>"),
            tracemalloc.Filter(False, "<unknown>"),
            tracemalloc.Filter(False, tracemalloc.__file__),
        )
    )


@contextlib.contextmanager
def trace_mem(activated=False):
    if not activated:
        yield
    else:
        hp = hpy()
        heap_before = hp.heap()
        before = _snapshot()
        try:
            yield
        finally:
            after = _snapshot()
            heap_after = hp.heap()
            leftover = heap_after - heap_before
            logger.info(leftover)
            largest = after.statistics("traceback")[0]
            logger.info("===> Largest memory usage:")
            for line in largest.traceback.format():
                logger.info(line)
            logger.info("<===")
            stats = after.statistics("filename")
            logger.info("===> Top 5 stats grouped by filename")
            for s in stats[:5]:
                logger.info(s)
            logger.info("<===")
            top_stats = after.compare_to(before, "lineno")
            logger.info("===> Memory snapshot diff top 5")
            for stat in top_stats[:5]:
                logger.info(stat)
            logger.info("<===")


def get_size(ob):
    """Returns size in Bytes"""
    return asizeof.asizeof(ob)


def get_base64_value(content):
    """
    Returns the converted file passed into a base64 encoded value
    Args:
           content (byte): Object content in bytes
    """
    return base64.b64encode(content).decode("utf-8")


class MemQueue(asyncio.Queue):
    def __init__(
        self, maxsize=0, maxmemsize=0, refresh_interval=1.0, refresh_timeout=60
    ):
        super().__init__(maxsize)
        self.maxmemsize = maxmemsize
        self.refresh_interval = refresh_interval
        self._current_memsize = 0
        self.refresh_timeout = refresh_timeout

    def _get(self):
        item_size, item = self._queue.popleft()
        self._current_memsize -= item_size
        return item_size, item

    def _put(self, item):
        self._current_memsize += item[0]
        self._queue.append(item)

    def mem_full(self):
        if self.maxmemsize == 0:
            return False
        return self.qmemsize() >= self.maxmemsize

    def qmemsize(self):
        return self._current_memsize

    async def _wait_for_room(self, item):
        item_size = get_size(item)
        if self._current_memsize + item_size <= self.maxmemsize:
            return item_size
        start = time.time()
        while self._current_memsize + item_size >= self.maxmemsize:
            if time.time() - start >= self.refresh_timeout:
                raise asyncio.QueueFull()
            logger.debug("Queue Full")
            await asyncio.sleep(self.refresh_interval)
        return item_size

    async def put(self, item):
        item_size = await self._wait_for_room(item)
        return await super().put((item_size, item))


class ConcurrentTasks:
    """Async task manager.

    Can be used to trigger concurrent async tasks with a maximum
    concurrency value.

    - `max_concurrency`: max concurrent tasks allowed, default: 5
    - `results_callback`: when provided, synchronous funciton called with the result of each task.
    """

    def __init__(self, max_concurrency=5, results_callback=None):
        self.max_concurrency = max_concurrency
        self.tasks = []
        self.results_callback = results_callback
        self._task_over = asyncio.Event()

    def __len__(self):
        return len(self.tasks)

    def _callback(self, task, result_callback=None):
        self.tasks.remove(task)
        self._task_over.set()
        if task.exception():
            raise task.exception()
        if result_callback is not None:
            result_callback(task.result())
        # global callback
        if self.results_callback is not None:
            self.results_callback(task.result())

    async def put(self, coroutine, result_callback=None):
        """Adds a coroutine for immediate execution.

        If the number of running tasks reach `max_concurrency`, this
        function will block and wait for a free slot.

        If provided, `result_callback` will be called when the task is done.
        """
        # If self.tasks has reached its max size, we wait for one task to finish
        if len(self.tasks) >= self.max_concurrency:
            await self._task_over.wait()
            # rearm
            self._task_over.clear()
        task = asyncio.create_task(coroutine())
        self.tasks.append(task)
        task.add_done_callback(
            functools.partial(self._callback, result_callback=result_callback)
        )
        return task

    async def join(self):
        """Wait for all tasks to finish."""
        await asyncio.gather(*self.tasks)
