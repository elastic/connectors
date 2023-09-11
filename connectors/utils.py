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
import shutil
import ssl
import subprocess
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from enum import Enum

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientConnectionError, ServerTimeoutError
from base64io import Base64IO
from bs4 import BeautifulSoup
from cstriggers.core.trigger import QuartzCron
from pympler import asizeof

from connectors.logger import logger

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


def iso_utc(when=None):
    if when is None:
        when = datetime.now(timezone.utc)
    return when.isoformat()


def next_run(quartz_definition):
    """Returns the datetime of the next run."""
    cron_obj = QuartzCron(quartz_definition, datetime.utcnow())
    return cron_obj.next_trigger()


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


def decode_base64_value(content):
    """
    Decodes the base64 encoded content
    Args:
           content (string): base64 encoded content
    """
    return base64.b64decode(content)


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
        item_size, item = self._queue.popleft()  # pyright: ignore
        self._current_memsize -= item_size
        return item_size, item

    def _put(self, item):
        self._current_memsize += item[0]  # pyright: ignore
        self._queue.append(item)  # pyright: ignore

    def full(self, next_item_size=0):
        full_by_numbers = super().full()

        if full_by_numbers:
            return True

        # Fun stuff here: if we don't have anything in-memory
        # then it's okay to put any object inside - it's already in memory
        # so we won't overload memory too much
        if self._current_memsize == 0:
            return False

        return self._current_memsize + next_item_size >= self.maxmemsize

    async def _putter_timeout(self, putter):
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

    def put_nowait(self, item):
        item_size = get_size(item)
        if self.full(item_size):
            raise asyncio.QueueFull(
                f"Queue is full: attempting to add item of size {item_size} bytes while {self.maxmemsize - self._current_memsize} free bytes left."
            )
        super().put_nowait((item_size, item))


class ConcurrentTasks:
    """Async task manager.

    Can be used to trigger concurrent async tasks with a maximum
    concurrency value.

    - `max_concurrency`: max concurrent tasks allowed, default: 5
    - `results_callback`: when provided, synchronous function called with the result of each task.
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
            logger.error(
                f"Exception found for task {task.get_name()}: {task.exception()}",
                exc_info=True,
            )
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
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def cancel(self):
        """Cancels all tasks"""
        for task in self.tasks:
            task.cancel()


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


class RetryStrategy(Enum):
    CONSTANT = 0
    LINEAR_BACKOFF = 1
    EXPONENTIAL_BACKOFF = 2


class UnknownRetryStrategyError(Exception):
    pass


def retryable(
    retries=3,
    interval=1.0,
    strategy=RetryStrategy.LINEAR_BACKOFF,
    skipped_exceptions=None,
):
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
            raise NotImplementedError(
                f"Retryable decorator is not implemented for {func.__class__}."
            )

    return wrapper


def retryable_async_function(func, retries, interval, strategy, skipped_exceptions):
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
                await asyncio.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_async_generator(func, retries, interval, strategy, skipped_exceptions):
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
                await asyncio.sleep(
                    time_to_sleep_between_retries(strategy, interval, retry)
                )
                retry += 1

    return wrapped


def retryable_sync_function(func, retries, interval, strategy, skipped_exceptions):
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


def time_to_sleep_between_retries(strategy, interval, retry):
    match strategy:
        case RetryStrategy.CONSTANT:
            return interval
        case RetryStrategy.LINEAR_BACKOFF:
            return interval * retry
        case RetryStrategy.EXPONENTIAL_BACKOFF:
            return interval**retry
        case _:
            raise UnknownRetryStrategyError()


def ssl_context(certificate):
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


def url_encode(original_string):
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


def evaluate_timedelta(seconds, time_skew=0):
    """Adds seconds to the current utc time.

    Args:
        seconds (int): Number of seconds to add in current time
        time_skew (int): Time of clock skew. Defaults to 0
    """
    modified_time = datetime.utcnow() + timedelta(seconds=seconds)
    # account for clock skew
    modified_time -= timedelta(seconds=time_skew)
    return iso_utc(when=modified_time)


def is_expired(expires_at):
    """Compares the given time with present time

    Args:
        expires_at (datetime): Time to check if expired.
    """
    # Recreate in case there's no expires_at present
    if expires_at is None:
        return True
    return datetime.utcnow() >= expires_at


def get_pem_format(key, postfix="-----END CERTIFICATE-----"):
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


def hash_id(_id):
    # Collision probability: 1.47*10^-29
    return hashlib.md5(_id.encode("utf8")).hexdigest()


def truncate_id(_id):
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


def has_duplicates(strings_list):
    seen = set()
    for string in strings_list:
        if string in seen:
            return True
        seen.add(string)
    return False


def filter_nested_dict_by_keys(key_list, source_dict):
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


def deep_merge_dicts(base_dict, new_dict):
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
    cache.set_value(50, datetime.datetime.now() + datetime.timedelta(5)
    value = cache.get() # 50
    sleep(5)
    value = cache.get() # None
    """

    def __init__(self):
        self._value = None
        self._expiration_date = None

    def get_value(self):
        """Get the value that's stored inside if it hasn't expired.

        If the expiration_date is past due, None is returned instead.
        """
        if self._value:
            if not is_expired(self._expiration_date):
                return self._value

        self._value = None

        return None

    def set_value(self, value, expiration_date):
        """Set the value in the cache with expiration date.

        Once expiration_date is past due, the value will be lost.
        """
        self._value = value
        self._expiration_date = expiration_date


def html_to_text(html):
    if not html:
        return html
    try:
        return BeautifulSoup(html, "lxml").get_text(separator="\n")
    except Exception:
        # TODO: figure out which exceptions can be thrown
        # we actually don't want to raise, just fall back to bs4
        return BeautifulSoup(html, features="html.parser").get_text(separator="\n")


async def aenumerate(asequence, start=0):
    i = start
    async for elem in asequence:
        try:
            yield i, elem
        finally:
            i += 1


def iterable_batches_generator(iterable, batch_size):
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


def dict_slice(hsh, keys, default=None):
    """
    Slice a dict by a subset of its keys.
    :param hsh: The input dictionary to slice
    :param keys: The desired keys from that dictionary. If any key is not present in hsh, the default value will be stored in the result.
    :return: A new dict with only the subset of keys
    """
    return {k: hsh.get(k, default) for k in keys}


class ExtractionService:
    """Data extraction service manager

    Calling `extract_text` with a filename will begin text extraction
    using an instance of the data extraction service.
    Requires the data extraction service to be running
    """

    __EXTRACTION_CONFIG = {}  # setup by cli.py on startup

    @classmethod
    def get_extraction_config(cls):
        return __EXTRACTION_CONFIG

    @classmethod
    def set_extraction_config(cls, extraction_config):
        global __EXTRACTION_CONFIG
        __EXTRACTION_CONFIG = extraction_config

    def __init__(self):
        self.session = None

        self.extraction_config = ExtractionService.get_extraction_config()
        if self.extraction_config is not None:
            self.host = self.extraction_config.get("host", None)
            self.timeout = self.extraction_config.get("timeout", 30)
            self.headers = {"accept": "application/json"}
            self.chunk_size = self.extraction_config.get("stream_chunk_size", 65536)

            self.use_file_pointers = self.extraction_config.get(
                "use_file_pointers", False
            )
            if self.use_file_pointers:
                self.volume_dir = self.extraction_config.get(
                    "shared_volume_dir", "/app/files"
                )
            else:
                self.volume_dir = None
                self.headers["content-type"] = "application/octet-stream"
        else:
            self.host = None

        if self.host is None:
            logger.warning(
                "Extraction service has been initialised but no extraction service configuration was found. No text will be extracted for this sync."
            )

    def _check_configured(self):
        if self.host is not None:
            return True

        return False

    def _begin_session(self):
        if self.session is not None:
            return self.session

        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers=self.headers,
        )

    async def _end_session(self):
        if not self.session:
            return

        await self.session.close()

    def get_volume_dir(self):
        if self.host is None:
            return None

        return self.volume_dir

    async def extract_text(self, filepath, original_filename):
        """Sends a text extraction request to tika-server using the supplied filename.
        Args:
            filepath: local path to the tempfile for extraction
            original_filename: original name of file

        Returns the extracted text
        """

        content = ""

        if self._check_configured() is False:
            # an empty host means configuration was not set correctly
            # a warning is already raised in __init__
            return content

        if self.session is None:
            self._begin_session()

        filename = (
            original_filename if original_filename else os.path.basename(filepath)
        )

        try:
            if self.use_file_pointers:
                content = await self.send_filepointer(filepath, original_filename)
            else:
                content = await self.send_file(filepath, original_filename)
        except (ClientConnectionError, ServerTimeoutError) as e:
            logger.error(
                f"Connection to {self.host} failed while extracting data from {filename}. Error: {e}"
            )
        except Exception as e:
            logger.error(
                f"Text extraction unexpectedly failed for {filename}. Error: {e}"
            )

        return content

    async def send_filepointer(self, filepath, filename):
        async with self._begin_session().put(
            f"{self.host}/extract_text/?local_file_path={filepath}",
        ) as response:
            return await self.parse_extraction_resp(filename, response)

    async def send_file(self, filepath, filename):
        async with self._begin_session().put(
            f"{self.host}/extract_text/",
            data=self.file_sender(filepath),
        ) as response:
            return await self.parse_extraction_resp(filename, response)

    async def file_sender(self, filepath):
        async with aiofiles.open(filepath, "rb") as f:
            chunk = await f.read(self.chunk_size)
            while chunk:
                yield chunk
                chunk = await f.read(self.chunk_size)

    async def parse_extraction_resp(self, filename, response):
        """Parses the response from the tika-server and logs any extraction failures.

        Returns `extracted_text` from the response.
        """
        content = await response.json(content_type=None)

        if response.status != 200 or content.get("error"):
            logger.warning(
                f"Extraction service could not parse `{filename}'. Status: [{response.status}]; {content.get('error', 'unexpected error')}: {content.get('message', 'unknown cause')}"
            )
            return ""

        logger.debug(f"Text extraction is successful for '{filename}'.")
        return content.get("extracted_text", "")


def base64url_to_base64(string):
    if string is None:
        return string

    if len(string) == 0:
        return ""

    string = string.replace("-", "+")
    return string.replace("_", "/")


def validate_email_address(email_address):
    """Validates an email address against a regular expression.
    This method does not include any remote check against an SMTP server for example."""

    # non None values indicate a match
    return re.fullmatch(EMAIL_REGEX_PATTERN, email_address) is not None
