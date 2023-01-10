#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import contextlib
import functools
import gc
import os
import platform
import shutil
import subprocess
import time
import tracemalloc
from datetime import datetime, timezone

from base64io import Base64IO
from cstriggers.core.trigger import QuartzCron
from guppy import hpy
from pympler import asizeof

from connectors.logger import logger

DEFAULT_CHUNK_SIZE = 500
DEFAULT_QUEUE_SIZE = 1024
DEFAULT_DISPLAY_EVERY = 100
DEFAULT_QUEUE_MEM_SIZE = 5
DEFAULT_CHUNK_MEM_SIZE = 25
DEFAULT_MAX_CONCURRENCY = 5
DEFAULT_CONCURRENT_DOWNLOADS = 10
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


_BASE64 = shutil.which("base64")


def convert_to_b64(source, target=None, overwrite=False):
    """Converts a `source` file to base64 using the system's `base64`

    When `target` is not provided, done in-place.

    If `overwrite` is `True` and `target` exists, overwrites it.
    If `False` and it exists, raises an `IOError`

    If the `base64` utility could not be found, falls back to pure Python
    using base64io.

    This function blocks -- if you want to avoid blocking the event
    loop, call it through `loop.run_in_executor`

    Returns the target file.
    """
    inplace = target is None
    temp_target = f"{source}.b64"
    if not inplace and not overwrite and os.path.exists(target):
        raise IOError(f"{target} already exists.")

    if _BASE64 is not None:
        if platform.system() == "Darwin":
            cmd = f"{_BASE64} {source} > {temp_target}"
        else:
            # In Linuces, avoid line wrapping
            cmd = f"{_BASE64} -w 0 {source} > {temp_target}"
        subprocess.check_call(cmd, shell=True)
    else:
        # Pure Python version
        with open(source, "rb") as sf, open(temp_target, "wb") as tf:
            with Base64IO(tf) as encoded_target:
                for line in sf:
                    encoded_target.write(line)

    # success, let's move the file to the right place
    if inplace:
        os.remove(source)
        os.rename(temp_target, source)
    else:
        if os.path.exists(target):
            os.remove(target)
        os.rename(temp_target, target)

    return source if inplace else target


class MemQueue(asyncio.Queue):
    def __init__(
        self, maxsize=0, maxmemsize=0, refresh_interval=1.0, refresh_timeout=60
    ):
        super().__init__(maxsize)
        self.maxmemsize = maxmemsize
        self.refresh_interval = refresh_interval
        self._current_memsize = 0
        self.refresh_timeout = refresh_timeout

    def qmemsize(self):
        return self._current_memsize

    def _get(self):
        item_size, item = self._queue.popleft()
        self._current_memsize -= item_size
        return item_size, item

    def _put(self, item):
        self._current_memsize += item[0]
        self._queue.append(item)

    def full(self, next_item_size=0):
        full_by_numbers = super().full()
        if full_by_numbers:
            return True
        return self._current_memsize + next_item_size >= self.maxmemsize

    async def _putter_timeout(self, putter):
        """This coroutine will set the result of the putter to QueueFull when a certain timeout it reached."""
        start = time.time()
        while not putter.done():
            if time.time() - start >= self.refresh_timeout:
                putter.set_result(asyncio.QueueFull())
                return
            logger.debug("Queue Full")
            await asyncio.sleep(self.refresh_interval)

    async def put(self, item):
        item_size = get_size(item)

        # This block is taken from the original put() method but with two
        # changes:
        #
        # 1/ full() takes the new item size to decide if we're going over the
        #    max size, so we do a single call on `get_size` per item
        #
        # 2/ when the putter is done, we check if the result is QueueFull.
        #    if it's the case, we re-raise it here
        while self.full(item_size):
            #
            # self._putter is a deque used as a FIFO queue by asyncio.Queue.
            #
            # Everytime a item is to be added in a full queue, a future (putter)
            # is added at the end of that deque. A `get` call on the queue will remove the
            # fist element in that deque and set the future result, and this
            # will unlock the corresponding put() call here.
            #
            # This mechanism ensures that we serialize put() calls when the queue is full.
            putter = self._get_loop().create_future()
            putter_timeout = self._get_loop().create_task(self._putter_timeout(putter))
            self._putters.append(putter)
            try:
                result = await putter
                if isinstance(result, asyncio.QueueFull):
                    raise result
            except:  # NOQA
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    # Clean self._putters from canceled putters.
                    self._putters.remove(putter)
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass
                if not self.full() and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._putters)
                raise

            await putter_timeout

        super().put_nowait((item_size, item))

    def put_nowait(self, item):
        item_size = get_size(item)
        if self.full(item_size):
            raise asyncio.QueueFull
        super().put_nowait((item_size, item))


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


def get_event_loop(uvloop=False):
    if uvloop:
        # activate uvloop if lib is present
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except Exception:
            pass
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop is None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    return loop
