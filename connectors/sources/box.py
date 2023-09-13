#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Box source module responsible to fetch documents from Box"""
import asyncio
from datetime import datetime, timedelta
from functools import cached_property, partial

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CacheWithTimeout,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    RetryStrategy,
    convert_to_b64,
    retryable,
)

ENDPOINTS = {
    "TOKEN": "/oauth2/token",
    "PING": "/2.0/users/me",
    "FOLDER": "/2.0/folders/{folder_id}/items",
    "CONTENT": "/2.0/files/{file_id}/content",
}
RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760
CHUNK_SIZE = 1024
FETCH_LIMIT = 1000
QUEUE_MEM_SIZE = 5 * 1024 * 1024  # ~ 5 MB
MAX_CONCURRENCY = 2000
CONCURRENT_DOWNLOADS = 15
FIELDS = "name,modified_at,download_url,size,type,sequence_id,etag,created_at,modified_at,content_created_at,content_modified_at,description,created_by,modified_by,owned_by,parent,item_status"
FILE = "file"

refresh_token = None


class TokenError(Exception):
    pass


class NotFound(Exception):
    pass


class AccessToken:
    def __init__(self, configuration, http_session):
        global refresh_token
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self._http_session = http_session
        if refresh_token is None:
            refresh_token = configuration["refresh_token"]
        self._token_cache = CacheWithTimeout()

    async def get(self):
        if cached_value := self._token_cache.get_value():
            return cached_value
        await self._set_access_token()
        return self.access_token

    async def _set_access_token(self):
        global refresh_token
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            async with self._http_session.post(
                url=ENDPOINTS["TOKEN"],
                data=data,
            ) as response:
                tokens = await response.json()
                self.access_token = tokens.get("access_token")
                refresh_token = tokens.get("refresh_token")
                self.expired_at = datetime.utcnow() + timedelta(
                    seconds=int(tokens.get("expires_in", 3599))
                )
                self._token_cache.set_value(
                    value=self.access_token, expiration_date=self.expired_at
                )
        except Exception as exception:
            raise TokenError(
                f"Error while generating access token. Please verify that provided configurations are correct. Exception {exception}."
            ) from exception


class BoxClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self._http_session = aiohttp.ClientSession(
            base_url="https://api.box.com", raise_for_status=True
        )
        self.token = AccessToken(
            configuration=configuration, http_session=self._http_session
        )

    def set_logger(self, logger_):
        self._logger = logger_

    async def _put_to_sleep(self, exception):
        retry_after = int(exception.headers.get("retry-after", 5))
        self._logger.debug(
            f"Connector will attempt to retry after {retry_after} seconds."
        )
        await self._sleeps.sleep(retry_after)
        raise Exception("Rate limit exceeded.")

    async def _handle_client_errors(self, exception):
        match exception.status:
            case 401:
                await self.token._set_access_token()
                raise
            case 429:
                await self._put_to_sleep(exception=exception)
                raise
            case 404:
                raise NotFound(f"Resource Not Found. Error: {exception}")
            case _:
                raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def get(self, url, params=None):
        try:
            access_token = await self.token.get()
            headers = {
                "Authorization": f"Bearer {access_token}",
            }
            return await self._http_session.get(url=url, headers=headers, params=params)
        except ClientResponseError as exception:
            await self._handle_client_errors(exception=exception)
        except Exception:
            raise

    async def paginated_call(self, url, params):
        try:
            offset = 0
            while True:
                params.update({"offset": offset, "limit": FETCH_LIMIT})
                response = await self.get(url=url, params=params)
                json_response = await response.json()
                total_count = json_response.get("total_count")
                for doc in json_response.get("entries"):
                    yield doc
                if offset >= total_count:
                    break
                offset += FETCH_LIMIT
        except Exception:
            raise

    async def ping(self):
        await self.get(url=ENDPOINTS["PING"])

    async def close(self):
        self._sleeps.cancel()
        await self._http_session.close()


class BoxDataSource(BaseDataSource):
    name = "Box"
    service_type = "box"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.tasks = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

    def _set_internal_logger(self):
        self.client.set_logger(logger_=self._logger)

    @cached_property
    def client(self):
        return BoxClient(configuration=self.configuration)

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Box

        Args:
            options (dict): Config bulker options.
        """
        options["concurrent_downloads"] = CONCURRENT_DOWNLOADS

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Box.

        Returns:
            dict: Default configuration.
        """
        return {
            "client_id": {
                "label": "Client ID",
                "order": 1,
                "type": "str",
            },
            "client_secret": {
                "label": "Client Secret",
                "order": 2,
                "sensitive": True,
                "type": "str",
            },
            "refresh_token": {
                "label": "Refresh Token",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
        }

    async def close(self):
        await self.client.close()

    async def ping(self):
        try:
            await self.client.ping()
            self._logger.info("Successfully connected to Box.")
        except Exception:
            self._logger.exception("Error while connecting to Box.")
            raise

    async def _fetch_files_folders(self, doc_id):
        try:
            params = {
                "fields": FIELDS,
            }
            async for folder_entry in self.client.paginated_call(
                url=ENDPOINTS["FOLDER"].format(folder_id=doc_id), params=params
            ):
                doc = folder_entry.copy()
                doc["_id"] = doc.pop("id")
                doc["_timestamp"] = doc.pop("modified_at")
                if folder_entry.get("type") == FILE:
                    doc.pop("download_url")
                    await self.queue.put(
                        (doc, partial(self.get_content, attachment=folder_entry))
                    )
                else:
                    await self.queue.put((doc, None))
                    await self.fetchers.put(
                        partial(self._fetch_files_folders, folder_entry.get("id"))
                    )
                    self.tasks += 1
            await self.queue.put("FINISHED")
        except Exception as exception:
            self._logger.info(
                f"Something went wrong while fetching data from the folder ID: {doc_id}. Error: {exception}"
            )

    async def _get_document_with_content(self, url, attachment_name, document):
        file_data = await self.client.get(url=url)
        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for data in file_data.content.iter_chunked(CHUNK_SIZE):
                await async_buffer.write(data)
            temp_filename = str(async_buffer.name)

        self._logger.debug(f"Calling convert_to_b64 for file : {attachment_name}")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)
        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await async_buffer.read()).strip()

        try:
            await remove(temp_filename)
        except Exception as exception:
            self._logger.warning(
                f"Could not remove file from: {temp_filename}. Error: {exception}"
            )
        return document

    def _pre_checks_for_get_content(
        self, attachment_extension, attachment_name, attachment_size
    ):
        if attachment_extension == "":
            self._logger.debug(
                f"Files without extension are not supported, skipping {attachment_name}."
            )
            return False

        elif attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(
                f"Files with the extension {attachment_extension} are not supported, skipping {attachment_name}."
            )
            return False

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return False
        return True

    async def get_content(self, attachment, timestamp=None, doit=False):
        """Extracts the content for Apache TIKA supported file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        attachment_size = int(attachment["size"])
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment["name"]
        attachment_extension = (
            attachment_name[attachment_name.rfind(".") :]
            if "." in attachment_name
            else ""
        )

        if not self._pre_checks_for_get_content(
            attachment_extension=attachment_extension,
            attachment_name=attachment_name,
            attachment_size=attachment_size,
        ):
            return

        self._logger.debug(f"Downloading {attachment_name}")
        document = {
            "_id": attachment["id"],
            "_timestamp": attachment["modified_at"],
        }
        return await self._get_document_with_content(
            url=ENDPOINTS["CONTENT"].format(file_id=attachment["id"]),
            attachment_name=attachment_name,
            document=document,
        )

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Box.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()
            if item == "FINISHED":
                self.tasks -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        # "0" refers to the root folder
        await self.fetchers.put(partial(self._fetch_files_folders, "0"))
        self.tasks += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
