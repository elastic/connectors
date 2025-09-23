#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import base64
import functools
import hashlib
import inspect
import os
import platform
import re
import secrets
import shutil
import ssl
import string
import subprocess  # noqa S404
import time
import urllib.parse
from _asyncio import Future, Task
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from enum import Enum
from time import strftime
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)
from unittest.mock import AsyncMock, Mock

import dateutil.parser as parser
import pytz
import tzcron
from base64io import Base64IO
from bs4 import BeautifulSoup
from freezegun.api import FakeDatetime
from pympler import asizeof
from typing_extensions import Buffer

from connectors.exceptions import DocumentIngestionError
from connectors.logger import _TracedAsyncGenerator, logger

ACCESS_CONTROL_INDEX_PREFIX = ".search-acl-filter-"
DEFAULT_CHUNK_SIZE = 500
DEFAULT_QUEUE_SIZE = 1024
DEFAULT_DISPLAY_EVERY = 100
DEFAULT_QUEUE_MEM_SIZE = 5
DEFAULT_CHUNK_MEM_SIZE = 25
DEFAULT_MAX_CONCURRENCY = 5
DEFAULT_CONCURRENT_DOWNLOADS = 10

# Regular expression pattern to match a basic email format (no whitespace, valid domain)
EMAIL_REGEX_PATTERN = r"^\S+@\S+\.\S+$"

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
    ".aspx",
    ".xlsb",
    ".xlsm",
    ".tsv",
    ".svg",
    ".msg",
    ".potx",
    ".vsd",
    ".vsdx",
    ".vsdm",
]

ISO_ZULU_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class Format(Enum):
    VERBOSE = "verbose"
    SHORT = "short"


def parse_datetime_string(datetime: str) -> datetime:
    return parser.parse(datetime)


def iso_utc(when: Optional[datetime] = None) -> str:
    if when is None:
        when = datetime.now(timezone.utc)
    return when.isoformat()


def with_utc_tz(ts: datetime) -> datetime:
    """Ensure the timestmap has a timezone of UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    else:
        return ts.astimezone(timezone.utc)


def iso_zulu() -> str:
    """Returns the current time in ISO Zulu format"""
    return datetime.now(timezone.utc).strftime(ISO_ZULU_TIMESTAMP_FORMAT)


def epoch_timestamp_zulu() -> str:
    """Returns the timestamp of the start of the epoch, in ISO Zulu format"""
    return strftime(ISO_ZULU_TIMESTAMP_FORMAT, time.gmtime(0))


def next_run(quartz_definition: str, now: datetime) -> datetime:
    """Returns the datetime in UTC timezone of the next run."""
    # Year is optional and is never present.
    _, minutes, hours, day_of_month, month, day_of_week, year = (
        quartz_definition.split(" ") + [None]
    )[:7]

    # Day of week is 1-7 starting from Sunday in Quartz and from Monday in regular Cron, adjust
    # Days before: 1 - SUN, 2 - MON ... 7 - SAT
    # Days after: 1 - MON, 2 - TUE ... 7 - SUN
    if day_of_week.isnumeric():
        day_of_week = (int(day_of_week) - 2) % 7 + 1

    if not year:
        year = "*"

    # tzcron always expects year
    repackaged_definition = (
        f"{minutes} {hours} {day_of_month} {month} {day_of_week} {year}"
    )

    # ? comes from Quartz Cron, regular cron doesn't handle it well
    repackaged_definition = repackaged_definition.replace("?", "*")

    schedule = tzcron.Schedule(repackaged_definition, pytz.utc, now)

    next_occurrence = next(schedule)
    return with_utc_tz(next_occurrence)


INVALID_CHARS = "\\", "/", "*", "?", '"', "<", ">", "|", " ", ",", "#"
INVALID_PREFIX = "_", "-", "+"
INVALID_NAME = "..", "."


class InvalidIndexNameError(ValueError):
    pass


def validate_index_name(name: str) -> str:
    for char in INVALID_CHARS:
        if char in name:
            msg = f"Invalid character {char}"
            raise InvalidIndexNameError(msg)

    if name.startswith(INVALID_PREFIX):
        msg = f"Invalid prefix {name[0]}"
        raise InvalidIndexNameError(msg)

    if name in INVALID_NAME:
        msg = "Can't use that name"
        raise InvalidIndexNameError(msg)

    if not name.islower():
        msg = "Must be lowercase"
        raise InvalidIndexNameError(msg)

    return name


class CancellableSleeps:
    def __init__(self) -> None:
        self._sleeps = set()

    async def sleep(
        self, delay: Union[float, Mock, int], result: None = None, *, loop=None
    ) -> None:
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

    def cancel(self, sig: None = None) -> None:
        if sig:
            logger.debug(f"Caught {sig}. Cancelling sleeps...")
        else:
            logger.debug("Cancelling sleeps...")

        for task in self._sleeps:
            task.cancel()


def get_size(ob: Any) -> int:
    """Returns size in Bytes"""
    return asizeof.asizeof(ob)


def get_file_extension(filename: str) -> str:
    return os.path.splitext(filename)[-1]


def get_base64_value(content: Buffer) -> str:
    """
    Returns the converted file passed into a base64 encoded value
    Args:
           content (byte): Object content in bytes
    """
    return base64.b64encode(content).decode("utf-8")


def decode_base64_value(content: Union[str, Buffer]) -> bytes:
    """
    Decodes the base64 encoded content
    Args:
           content (string): base64 encoded content
    """
    return base64.b64decode(content)


_BASE64: Optional[str] = shutil.which("base64")


def convert_to_b64(
    source: Union[os.PathLike[bytes], os.PathLike[str], bytes, str],
    target: Union[None, os.PathLike[bytes], os.PathLike[str], bytes, int, str] = None,
    overwrite: bool = False,
) -> Union[
    os.PathLike[bytes],
    os.PathLike[str],
    os.PathLike[Union[bytes, str]],
    bytes,
    int,
    str,
    None,
]:
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
        msg = f"{target} already exists."
        raise IOError(msg)

    if _BASE64 is not None:
        if platform.system() == "Darwin":
            version = int(platform.mac_ver()[0].split(".")[0])
            # MacOS 13 has changed base64 util
            if version >= 13:
                cmd = f"{_BASE64} -i {source} -o {temp_target}"
            else:
                cmd = f"{_BASE64} {source} > {temp_target}"
        else:
            # In Linuces, avoid line wrapping
            cmd = f"{_BASE64} -w 0 {source} > {temp_target}"
        logger.debug(f"Calling {cmd}")
        subprocess.check_call(cmd, shell=True)  # noqa S602
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
        self,
        maxsize: int = 0,
        maxmemsize: int = 0,
        refresh_interval: float = 1.0,
        refresh_timeout: float = 60,
    ) -> None:
        super().__init__(maxsize)
        self.maxmemsize = maxmemsize
        self.refresh_interval = refresh_interval
        self._current_memsize = 0
        self.refresh_timeout = refresh_timeout

    def qmemsize(self) -> int:
        return self._current_memsize

    def _get(self) -> Any:
        item_size, item = self._queue.popleft()  # pyright: ignore
        self._current_memsize -= item_size
        return item_size, item

    def _put(self, item: Any) -> None:
        self._current_memsize += item[0]  # pyright: ignore
        self._queue.append(item)  # pyright: ignore

    def full(self, next_item_size: int = 0) -> bool:
        full_by_numbers = super().full()

        if full_by_numbers:
            return True

        # Fun stuff here: if we don't have anything in-memory
        # then it's okay to put any object inside - it's already in memory
        # so we won't overload memory too much
        if self._current_memsize == 0:
            return False

        return self._current_memsize + next_item_size >= self.maxmemsize

    async def _putter_timeout(self, putter: Future) -> None:
        """This coroutine will set the result of the putter to QueueFull when a certain timeout it reached."""
        start = time.time()
        while not putter.done():
            elapsed_time = time.time() - start
            if elapsed_time >= self.refresh_timeout:
                putter.set_result(
                    asyncio.QueueFull(
                        f"MemQueue has been full for {round(elapsed_time, 4)}s. while timeout is {self.refresh_timeout}s."
                    )
                )
                return
            logger.debug("Queue Full")
            await asyncio.sleep(self.refresh_interval)

    async def put(self, item: Any) -> None:
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
            # Every time a item is to be added in a full queue, a future (putter)
            # is added at the end of that deque. A `get` call on the queue will remove the
            # fist element in that deque and set the future result, and this
            # will unlock the corresponding put() call here.
            #
            # This mechanism ensures that we serialize put() calls when the queue is full.
            putter = self._get_loop().create_future()  # pyright: ignore
            putter_timeout = self._get_loop().create_task(  # pyright: ignore
                self._putter_timeout(putter)
            )
            self._putters.append(putter)  # pyright: ignore
            try:
                result = await putter
                if isinstance(result, asyncio.QueueFull):
                    raise result
            except:  # NOQA
                putter.cancel()  # Just in case putter is not done yet.
                try:
                    # Clean self._putters from canceled putters.
                    self._putters.remove(putter)  # pyright: ignore
                except ValueError:
                    # The putter could be removed from self._putters by a
                    # previous get_nowait call.
                    pass
                if not self.full() and not putter.cancelled():
                    # We were woken up by get_nowait(), but can't take
                    # the call.  Wake up the next in line.
                    self._wakeup_next(self._putters)  # pyright: ignore
                raise

            await putter_timeout

        super().put_nowait((item_size, item))

    def clear(self) -> None:
        while not self.empty():
            # Depending on your program, you may want to
            # catch QueueEmpty
            self.get_nowait()
            self.task_done()

    def put_nowait(self, item: Union[str, int]) -> None:
        item_size = get_size(item)
        if self.full(item_size):
            msg = f"Queue is full: attempting to add item of size {item_size} bytes while {self.maxmemsize - self._current_memsize} free bytes left."
            raise asyncio.QueueFull(msg)
        super().put_nowait((item_size, item))


class NonBlockingBoundedSemaphore(asyncio.BoundedSemaphore):
    """A bounded semaphore with non-blocking acquire implementation.

    This introduces a new try_acquire method, which will return if it can't acquire immediately.
    """

    def try_acquire(self) -> bool:
        if self.locked():
            return False

        self._value -= 1
        return True


class ConcurrentTasks:
    """Async task manager.

    Can be used to trigger concurrent async tasks with a maximum
    concurrency value.

    - `max_concurrency`: max concurrent tasks allowed, default: 5
    Examples:

        # create a task pool with the default max concurrency
        task_pool = ConcurrentTasks()

        # put a task into pool
        # it will block until the task was put successfully
        task = await task_pool.put(coroutine)

        # put a task without blocking
        # it will try to put the task, and return None if it can't be put immediately
        task = task_pool.try_put(coroutine)

        # call join to wait for all tasks in pool to complete
        # this is not required to execute the tasks in pool
        # a task will be automatically scheduled to execute once it's put successfully
        # call join() only when you need to do something after all tasks in pool complete
        await task_pool.join()
    """

    def __init__(self, max_concurrency: int = 5) -> None:
        self.tasks = []
        self._sem = NonBlockingBoundedSemaphore(max_concurrency)

    def __len__(self) -> int:
        return len(self.tasks)

    def _callback(self, task: Task) -> None:
        self.tasks.remove(task)
        self._sem.release()
        if task.cancelled():
            logger.error(
                f"Task {task.get_name()} was cancelled",
            )
        elif task.exception():
            logger.error(
                f"Exception found for task {task.get_name()}", exc_info=task.exception()
            )

    def _add_task(
        self, coroutine: Union[functools.partial, Callable], name: Optional[str] = None
    ) -> Task:
        task = asyncio.create_task(coroutine(), name=name)
        self.tasks.append(task)
        # _callback will be executed when the task is done,
        # i.e. the wrapped coroutine either returned a value, raised an exception, or the Task was cancelled.
        # Ref: https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.done
        task.add_done_callback(functools.partial(self._callback))
        return task

    async def put(
        self, coroutine: Union[functools.partial, Callable], name: Optional[str] = None
    ) -> Generator[Future, None, Task]:
        """Adds a coroutine for immediate execution.

        If the number of running tasks reach `max_concurrency`, this
        function will block and wait for a free slot.
        """
        await self._sem.acquire()
        return self._add_task(coroutine, name=name)

    def try_put(
        self, coroutine: functools.partial, name: Optional[str] = None
    ) -> Optional[Task]:
        """Tries to add a coroutine for immediate execution.

        If the number of running tasks reach `max_concurrency`, this
        function return a None task immediately
        """

        if self._sem.try_acquire():
            return self._add_task(coroutine, name=name)
        return None

    async def join(self, raise_on_error: bool = False) -> None:
        """Wait for all tasks to finish."""
        try:
            await asyncio.gather(*self.tasks, return_exceptions=(not raise_on_error))
        except:
            self.cancel()
            raise

    def raise_any_exception(self) -> None:
        for task in self.tasks:
            if task.done() and not task.cancelled():
                if task.exception():
                    logger.error(
                        f"Exception found for task {task.get_name()}: {task.exception()}",
                    )
                    self.cancel()  # cancel all the pending tasks
                    raise task.exception()

    def cancel(self) -> None:
        """Cancels all tasks"""
        for task in self.tasks:
            task.cancel()


class RetryStrategy(Enum):
    CONSTANT = 0
    LINEAR_BACKOFF = 1
    EXPONENTIAL_BACKOFF = 2


class UnknownRetryStrategyError(Exception):
    pass


sleeps_for_retryable = CancellableSleeps()


def retryable(
    retries: int = 3,
    interval: float = 1.0,
    strategy: RetryStrategy = RetryStrategy.LINEAR_BACKOFF,
    skipped_exceptions: Optional[Any] = None,
) -> Callable:
    def wrapper(func):
        if skipped_exceptions is None:
            processed_skipped_exceptions = []
        elif not isinstance(skipped_exceptions, list):
            processed_skipped_exceptions = [skipped_exceptions]
        else:
            processed_skipped_exceptions = skipped_exceptions

        if inspect.isasyncgenfunction(func):
            return retryable_async_generator(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        elif inspect.iscoroutinefunction(func):
            return retryable_async_function(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        elif inspect.isfunction(func):
            return retryable_sync_function(
                func, retries, interval, strategy, processed_skipped_exceptions
            )
        else:
            msg = f"Retryable decorator is not implemented for {func.__class__}."
            raise NotImplementedError(msg)

    return wrapper


def retryable_async_function(
    func: Callable,
    retries: int,
    interval: Union[float, int],
    strategy: RetryStrategy,
    skipped_exceptions: List[Any],
) -> Callable:
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e
                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                await sleeps_for_retryable.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_async_generator(
    func: Callable,
    retries: int,
    interval: Union[float, int],
    strategy: RetryStrategy,
    skipped_exceptions: List[Any],
) -> Callable:
    @functools.wraps(func)
    async def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                async for item in func(*args, **kwargs):
                    yield item
                break
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e

                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                await sleeps_for_retryable.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_sync_function(
    func: Callable,
    retries: int,
    interval: int,
    strategy: RetryStrategy,
    skipped_exceptions: List[Any],
) -> Callable:
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        retry = 1
        while retry <= retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if retry >= retries or e.__class__ in skipped_exceptions:
                    raise e
                logger.debug(
                    f"Retrying ({retry} of {retries}) with interval: {interval} and strategy: {strategy.name}"
                )
                time.sleep(time_to_sleep_between_retries(strategy, interval, retry))
                retry += 1

    return wrapped


def time_to_sleep_between_retries(
    strategy: Union[RetryStrategy, str], interval: Union[float, int], retry: int
) -> Union[float, int]:
    match strategy:
        case RetryStrategy.CONSTANT:
            return interval
        case RetryStrategy.LINEAR_BACKOFF:
            return interval * retry
        case RetryStrategy.EXPONENTIAL_BACKOFF:
            return interval**retry
        case _:
            raise UnknownRetryStrategyError()


def ssl_context(certificate: str) -> ssl.SSLContext:
    """Convert string to pem format and create a SSL context

    Args:
        certificate (str): certificate in string format

    Returns:
        ssl_context: SSL context with certificate
    """
    certificate = get_pem_format(certificate)
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cadata=certificate)
    return ctx


def url_encode(original_string: str) -> str:
    """Performs encoding on the objects
    containing special characters in their url, and
    replaces single quote with two single quote since quote
    is treated as an escape character

    Args:
        original_string(string): String containing special characters

    Returns:
        encoded_string(string): Parsed string without single quotes
    """
    return urllib.parse.quote(original_string, safe="'")


def evaluate_timedelta(seconds: int, time_skew: int = 0) -> str:
    """Adds seconds to the current utc time.

    Args:
        seconds (int): Number of seconds to add in current time
        time_skew (int): Time of clock skew. Defaults to 0
    """
    modified_time = datetime.utcnow() + timedelta(seconds=seconds)
    # account for clock skew
    modified_time -= timedelta(seconds=time_skew)
    return iso_utc(when=modified_time)


def is_expired(expires_at: Optional[Union[FakeDatetime, datetime]]) -> bool:
    """Compares the given time with present time

    Args:
        expires_at (datetime): Time to check if expired.
    """
    # Recreate in case there's no expires_at present
    if expires_at is None:
        return True
    return datetime.utcnow() >= expires_at


def get_pem_format(key: str, postfix: str = "-----END CERTIFICATE-----") -> str:
    """Convert key into PEM format.

    Args:
        key (str): Key in raw format.
        postfix (str): Certificate footer.

    Returns:
        string: PEM format

    Example:
        key = "-----BEGIN PRIVATE KEY----- PrivateKey -----END PRIVATE KEY-----"
        postfix = "-----END PRIVATE KEY-----"
        pem_format = "-----BEGIN PRIVATE KEY-----
                    PrivateKey
                    -----END PRIVATE KEY-----"
    """
    pem_format = ""
    reverse_split = postfix.count(" ")
    if key.count(postfix) == 1:
        key = key.replace(" ", "\n")
        key = " ".join(key.split("\n", reverse_split))
        key = " ".join(key.rsplit("\n", reverse_split))
        pem_format = key
    elif key.count(postfix) > 1:
        for cert in key.split(postfix)[:-1]:
            cert = cert.strip() + "\n" + postfix
            cert = cert.replace(" ", "\n")
            cert = " ".join(cert.split("\n", reverse_split))
            cert = " ".join(cert.rsplit("\n", reverse_split))
            pem_format += cert + "\n"
    return pem_format


def hash_id(_id: str) -> str:
    # Collision probability: 1.47*10^-29
    # S105 rule considers this code unsafe, but we're not using it for security-related
    # things, only to generate pseudo-ids for documents
    return hashlib.md5(_id.encode("utf8")).hexdigest()  # noqa S105


def truncate_id(_id: str) -> str:
    """Truncate ID of an object.

    We cannot guarantee that connector returns small IDs.
    In some places in our code we log IDs and if the ID is
    too big, these lines become unreadable.

    This function can help - it truncates the ID to not
    overwhelm the logging system and still have somewhat
    readable error messages.

    Args:
    _id (str): ID of an object to truncate.
    """

    if len(_id) > 20:
        return _id[:8] + "..." + _id[-8:]

    return _id


def has_duplicates(strings_list: List[Union[str, Any]]) -> bool:
    seen = set()
    for s in strings_list:
        if s in seen:
            return True
        seen.add(s)
    return False


def filter_nested_dict_by_keys(
    key_list: List[Union[str, Any]],
    source_dict: Dict[str, Union[Dict[str, int], Dict[Any, Any]]],
) -> Dict[str, Union[Dict[str, int], Dict[Any, Any]]]:
    """Filters a nested dict by the keys of the sub-level dict.
    This is used for checking if any configuration fields are missing properties.

    Args:
        key_list (list): list of keys to compare against nested dict keys
        source_dict (dict): a nested dict

    Returns a filtered nested dict.
    """
    filtered_dict = {}

    for top_key, nested_dict in source_dict.items():
        if key_list - nested_dict.keys():
            filtered_dict[top_key] = nested_dict

    return filtered_dict


def deep_merge_dicts(
    base_dict: Dict[str, Any], new_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """Deep merges two nested dicts.

    Args:
        base_dict (dict): dict that will be overwritten
        new_dict (dict): dict to be merged in

    Returns a merged nested dict.
    """
    for key in new_dict:
        if (
            key in base_dict
            and isinstance(base_dict[key], dict)
            and isinstance(new_dict[key], dict)
        ):
            deep_merge_dicts(base_dict[key], new_dict[key])
        else:
            base_dict[key] = new_dict[key]

    return base_dict


class CacheWithTimeout:
    """Structure to store an value that needs to expire. Some sort of L1 cache.

    Example of usage:

    cache = CacheWithTimeout()
    cache.set_value(50, datetime.now() + timedelta(5)
    value = cache.get() # 50
    sleep(5)
    value = cache.get() # None
    """

    def __init__(self) -> None:
        self._value = None
        self._expiration_date = None

    def get_value(self) -> Optional[str]:
        """Get the value that's stored inside if it hasn't expired.

        If the expiration_date is past due, None is returned instead.
        """
        if self._value:
            if not is_expired(self._expiration_date):
                return self._value

        self._value = None

        return None

    def set_value(
        self, value: str, expiration_date: Union[FakeDatetime, float, datetime]
    ) -> None:
        """Set the value in the cache with expiration date.

        Once expiration_date is past due, the value will be lost.
        """
        self._value = value
        self._expiration_date = expiration_date


def html_to_text(html: Optional[str]) -> Optional[Union[Mock, str]]:
    if not html:
        return html
    try:
        return BeautifulSoup(html, "lxml").get_text(separator="\n")
    except Exception:
        # TODO: figure out which exceptions can be thrown
        # we actually don't want to raise, just fall back to bs4
        return BeautifulSoup(html, features="html.parser").get_text(separator="\n")


async def aenumerate(asequence: _TracedAsyncGenerator, start: int = 0) -> None:
    i = start
    async for elem in asequence:
        try:
            yield i, elem
        finally:
            i += 1


def iterable_batches_generator(
    iterable: List[
        Union[Dict[str, str], Dict[str, Union[Dict[str, str], str]], int, Any]
    ],
    batch_size: int,
) -> Iterator[List[Union[Dict[str, str], Dict[str, Union[Dict[str, str], str]], int]]]:
    """Iterate over an iterable in batches.

    If the batch size is bigger than the number of remaining elements then all remaining elements will be returned.

    Args:
        iterable (iter): iterable (for example a list)
        batch_size (int): size of the returned batches

    Yields:
        batch (slice of the iterable): batch of size `batch_size`
    """

    num_items = len(iterable)
    for idx in range(0, num_items, batch_size):
        yield iterable[idx : min(idx + batch_size, num_items)]


def dict_slice(
    hsh: Dict[str, Union[bool, str, int]],
    keys: Union[
        Tuple[str, str, str, str],
        Tuple[str, str, str, str, str, str],
        Tuple[str, str, str, str, str, str, str],
    ],
    default: None = None,
) -> Dict[str, Optional[Union[str, int]]]:
    """
    Slice a dict by a subset of its keys.
    :param hsh: The input dictionary to slice
    :param keys: The desired keys from that dictionary. If any key is not present in hsh, the default value will be stored in the result.
    :return: A new dict with only the subset of keys
    """
    return {k: hsh.get(k, default) for k in keys}


def base64url_to_base64(string: Optional[str]) -> Optional[str]:
    if string is None:
        return string

    if len(string) == 0:
        return ""

    string = string.replace("-", "+")
    return string.replace("_", "/")


def validate_email_address(email_address: str) -> bool:
    """Validates an email address against a regular expression.
    This method does not include any remote check against an SMTP server for example."""

    # non None values indicate a match
    return re.fullmatch(EMAIL_REGEX_PATTERN, email_address) is not None


def shorten_str(string: Optional[str], shorten_by: int) -> str:
    """
    Shorten a string by removing characters from the middle, replacing them with '...'.

    If balanced shortening is not possible, retains an extra character at the beginning.

    Args:
        string (str): The string to be shortened.
        shorten_by (int): The number of characters to remove from the string.

    Returns:
        str: The shortened string.

    Examples:
        >>> shorten_str("abcdefgh", 1)
        'abcdefgh'
        >>> shorten_str("abcdefgh", 4)
        'ab...gh'
        >>> shorten_str("abcdefgh", 5)
        'ab...h'
        >>> shorten_str("abcdefgh", 1000)
        'a...h'
    """

    if string is None or string == "":
        return ""

    if not string or shorten_by < 3:
        return string

    length = len(string)
    shorten_by = min(length - 2, shorten_by)

    keep = (length - shorten_by) // 2
    balanced_shortening = (shorten_by + length) % 2 == 0

    if balanced_shortening:
        return f"{string[:keep]}...{string[-keep:]}"
    else:
        # keep one more at the front
        return f"{string[:keep + 1]}...{string[-keep:]}"


def func_human_readable_name(
    func: Union[AsyncMock, functools.partial, Callable],
) -> str:
    if isinstance(func, functools.partial):
        return func.func.__name__

    try:
        return func.__name__
    except AttributeError:
        return str(func)


def nested_get_from_dict(
    dictionary: Any,
    keys: Union[List[str], Tuple[str, str]],
    default: Optional[Union[bool, str, float]] = None,
) -> Any:
    def nested_get(dictionary_, keys_, default_=None):
        if dictionary_ is None:
            return default_

        if not keys_:
            return dictionary_

        if not isinstance(dictionary_, dict):
            return default_

        return nested_get(dictionary_.get(keys_[0]), keys_[1:], default_)

    return nested_get(dictionary, keys, default)


def sanitize(
    doc: Dict[str, Union[int, datetime, str]],
) -> Dict[str, Union[datetime, str]]:
    if doc["_id"]:
        # guarantee that IDs are strings, and not numeric
        doc["_id"] = str(doc["_id"])
        doc["id"] = doc["_id"]

    return doc


class Counters:
    """
    A utility to provide code readability to managing a collection of counts
    """

    def __init__(self) -> None:
        self._storage = {}

    def increment(
        self, key: str, value: int = 1, namespace: Optional[str] = None
    ) -> None:
        if namespace:
            key = f"{namespace}.{key}"
        self._storage[key] = self._storage.get(key, 0) + value

    def get(self, key: str) -> int:
        return self._storage.get(key, 0)

    def to_dict(self) -> Dict[str, int]:
        return deepcopy(self._storage)


class TooManyErrors(Exception):
    pass


class ErrorMonitor:
    def __init__(
        self,
        enabled: bool = True,
        max_total_errors: int = 1000,
        max_consecutive_errors: int = 10,
        max_error_rate: float = 0.15,
        error_window_size: int = 100,
        error_queue_size: int = 10,
    ) -> None:
        # When disabled, only track errors
        self.enabled = enabled

        self.max_error_rate = max_error_rate
        self.error_window_size = error_window_size
        self.error_window = [False] * error_window_size
        self.error_window_index = 0

        self.error_queue = []
        self.error_queue_size = error_queue_size

        self.consecutive_error_count = 0
        self.max_consecutive_errors = max_consecutive_errors

        self.max_total_errors = max_total_errors
        self.total_success_count = 0
        self.total_error_count = 0

    def track_success(self) -> None:
        self.consecutive_error_count = 0
        self.total_success_count += 1
        self._update_error_window(False)

    def track_error(
        self, error: Union[InvalidIndexNameError, DocumentIngestionError, Exception]
    ) -> None:
        self.total_error_count += 1
        self.consecutive_error_count += 1

        self.error_queue.append(error)

        if len(self.error_queue) > self.error_queue_size:
            self.error_queue.pop(0)

        self._update_error_window(True)
        self.last_error = error

        self._raise_if_necessary()

    def _update_error_window(self, value: bool) -> None:
        # We keep the errors array of the size self.error_window_size this way, imagine self.error_window_size = 5
        # Error array inits as falses:
        # [ false, false, false, false, false ]
        # Third document raises an error:
        # [ false, false, true,  false, false ]
        #                 ^^^^
        #                 2 % 5 == 2
        # Fifth document raises an error:
        # [ false, false, true,  false, true  ]
        #                               ^^^^
        #                               4 % 5 == 4
        # Sixth document raises an error:
        # [ true, false, true,   false, true  ]
        #   ^^^^
        #   5 % 5 == 0
        #
        # Eigth document is successful:
        # [ true, false, false,  false, true  ]
        #                ^^^^^
        #                7 % 5 == 2
        # And so on.
        self.error_window[self.error_window_index] = value
        self.error_window_index = (self.error_window_index + 1) % self.error_window_size

    def _error_window_error_rate(self) -> float:
        if self.error_window_size == 0:
            return 0

        errors = list(filter(lambda x: x is True, self.error_window))

        error_rate = len(errors) / self.error_window_size

        return error_rate

    def _raise_if_necessary(self) -> None:
        if not self.enabled:
            return

        if self.consecutive_error_count > self.max_consecutive_errors:
            msg = f"Exceeded maximum consecutive errors - saw {self.consecutive_error_count} errors in a row. Last error: {self.last_error}"
            raise TooManyErrors(msg) from self.last_error
        elif self.total_error_count > self.max_total_errors:
            msg = f"Exceeded maximum total error count - saw {self.total_error_count} errors. Last error: {self.last_error}"
            raise TooManyErrors(msg) from self.last_error
        elif self.error_window_size > 0:
            error_rate = self._error_window_error_rate()
            if error_rate > self.max_error_rate:
                msg = f"Exceeded maximum error ratio of {self.max_error_rate} for last {self.error_window_size} operations. Last error: {self.last_error}"
                raise TooManyErrors(msg) from self.last_error


def generate_random_id(length: int = 4) -> str:
    return "".join(
        secrets.choice(string.ascii_letters + string.digits) for _ in range(length)
    )
