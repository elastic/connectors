#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online.
"""
import asyncio
import json
import os
from datetime import datetime
from enum import Enum
from functools import cached_property, partial
from urllib import parse

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    RetryStrategy,
    convert_to_b64,
    evaluate_timedelta,
    is_expired,
    iso_utc,
    retryable,
)

RETRY_COUNT = 3
DEFAULT_RETRY_AFTER = 300  # seconds
RETRY_INTERVAL = 2
CHUNK_SIZE = 1024
FILE_SIZE_LIMIT = 10485760  # ~10 Megabytes
MAX_CONCURRENT_DOWNLOADS = 100
LIMIT = 300  # Limit for fetching shared files per call
PAPER = "paper"
FILE = "File"
FOLDER = "Folder"
RECEIVED_FILE = "Received File"

API_VERSION = 2

if "RUNNING_FTEST" in os.environ:
    logger.warning("x" * 100)
    logger.warning(
        f"DROPBOX CONNECTOR CALLS ARE REDIRECTED TO {os.environ['DROPBOX_API_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 100)
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": os.environ["DROPBOX_API_URL"],
        "FILES_FOLDERS_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
        "DOWNLOAD_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
    }
else:
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": "https://api.dropboxapi.com/",
        "FILES_FOLDERS_BASE_URL": f"https://api.dropboxapi.com/{API_VERSION}/",
        "DOWNLOAD_BASE_URL": f"https://content.dropboxapi.com/{API_VERSION}/",
    }


class EndpointName(Enum):
    ACCESS_TOKEN = "ACCESS_TOKEN"
    PING = "PING"
    CHECK_PATH = "CHECK_PATH"
    FILES_FOLDERS = "FILES_FOLDERS"
    FILES_FOLDERS_CONTINUE = "FILES_FOLDERS_CONTINUE"
    RECEIVED_FILES = "RECEIVED_FILES"
    RECEIVED_FILES_CONTINUE = "RECEIVED_FILES_CONTINUE"
    RECEIVED_FILE_METADATA = "RECEIVED_FILE_METADATA"
    DOWNLOAD = "DOWNLOAD"
    PAPER_FILE_DOWNLOAD = "PAPER_FILE_DOWNLOAD"
    RECEIVED_FILE_DOWNLOAD = "RECEIVED_FILE_DOWNLOAD"


ENDPOINTS = {
    EndpointName.ACCESS_TOKEN.value: "oauth2/token",
    EndpointName.PING.value: "users/get_current_account",
    EndpointName.CHECK_PATH.value: "files/get_metadata",
    EndpointName.FILES_FOLDERS.value: "files/list_folder",
    EndpointName.FILES_FOLDERS_CONTINUE.value: "files/list_folder/continue",
    EndpointName.RECEIVED_FILES.value: "sharing/list_received_files",
    EndpointName.RECEIVED_FILES_CONTINUE.value: "sharing/list_received_files/continue",
    EndpointName.RECEIVED_FILE_METADATA.value: "sharing/get_shared_link_metadata",
    EndpointName.DOWNLOAD.value: "files/download",
    EndpointName.PAPER_FILE_DOWNLOAD.value: "files/export",
    EndpointName.RECEIVED_FILE_DOWNLOAD.value: "sharing/get_shared_link_file",
}


class InvalidClientCredentialException(Exception):
    pass


class InvalidRefreshTokenException(Exception):
    pass


class InvalidPathException(Exception):
    pass


class BreakingField(Enum):
    CURSOR = "cursor"
    HAS_MORE = "has_more"


class DropboxClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.path = (
            ""
            if self.configuration["path"] in ["/", None]
            else self.configuration["path"]
        )
        self.retry_count = self.configuration["retry_count"]
        self.app_key = self.configuration["app_key"]
        self.app_secret = self.configuration["app_secret"]
        self.refresh_token = self.configuration["refresh_token"]
        self.access_token = None
        self.token_expiration_time = None
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        if self.token_expiration_time and (
            not isinstance(self.token_expiration_time, datetime)
        ):
            self.token_expiration_time = datetime.fromisoformat(
                self.token_expiration_time
            )
        if not is_expired(expires_at=self.token_expiration_time):
            return
        url = f"{BASE_URLS['ACCESS_TOKEN_BASE_URL']}{ENDPOINTS.get(EndpointName.ACCESS_TOKEN.value)}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.app_key,
            "client_secret": self.app_secret,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, data=data) as response:
                response_data = await response.json()
                if response.status != 200:
                    self.check_errors(response=response_data)
                self.access_token = response_data["access_token"]
                self.token_expiration_time = evaluate_timedelta(
                    seconds=int(response_data["expires_in"]), time_skew=20
                )
                self._logger.debug("Access Token generated successfully")

    def check_errors(self, response):
        error_response = response.get("error")
        if error_response == "invalid_grant":
            raise InvalidRefreshTokenException("Configured Refresh Token is invalid.")
        elif error_response == "invalid_client: Invalid client_id or client_secret":
            raise InvalidClientCredentialException(
                "Configured App Key or App Secret is invalid."
            )
        else:
            raise Exception(
                f"Error while generating an access token. Error: {error_response}"
            )

    @cached_property
    def _get_session(self):
        self._logger.debug("Generating aiohttp client session")
        timeout = aiohttp.ClientTimeout(total=None)

        return aiohttp.ClientSession(timeout=timeout, raise_for_status=True)

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def _get_request_headers(self, file_type, **kwargs):
        kwargs = kwargs["kwargs"]
        request_headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        if file_type == FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"path": "{kwargs["path"]}"}}'
        elif file_type == PAPER:
            request_headers[
                "Dropbox-API-Arg"
            ] = f'{{"path": "{kwargs["path"]}", "export_format": "markdown"}}'
        elif file_type == RECEIVED_FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"url": "{kwargs["url"]}"}}'
        else:
            request_headers["Content-Type"] = "application/json"
        return request_headers

    def _get_retry_after(self, retry, exception):
        self._logger.warning(
            f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
        )
        if retry >= self.retry_count:
            raise exception
        retry += 1
        return retry, RETRY_INTERVAL**retry

    async def _handle_client_errors(self, retry, exception):
        retry, retry_seconds = self._get_retry_after(retry=retry, exception=exception)
        match exception.status:
            case 401:
                await self._set_access_token()
            # For Dropbox APIs, 409 status code is responsible for endpoint-specific errors.
            # Refer `Errors by status code` section in the documentation: https://www.dropbox.com/developers/documentation/http/documentation
            case 409:
                raise InvalidPathException(
                    f"Configured Path: {self.path} is invalid or not found."
                ) from exception
            case 429:
                response_headers = exception.headers
                updated_response_headers = {
                    key.lower(): value for key, value in response_headers.items()
                }
                retry_seconds = int(
                    updated_response_headers.get("retry-after", DEFAULT_RETRY_AFTER)
                )
                self._logger.warning(
                    f"Rate limited by Dropbox. Retrying after {retry_seconds} seconds"
                )
            case _:
                raise
        self._logger.debug(f"Will retry in {retry_seconds} seconds")
        await self._sleeps.sleep(retry_seconds)
        return retry

    async def api_call(self, base_url, url_name, data=None, file_type=None, **kwargs):
        retry = 1
        url = parse.urljoin(base_url, url_name)
        while True:
            try:
                await self._set_access_token()
                headers = self._get_request_headers(file_type=file_type, kwargs=kwargs)
                async with self._get_session.post(
                    url=url, headers=headers, data=data
                ) as response:
                    yield response
                    break
            # These errors are handled in `check_errors` method
            except (InvalidClientCredentialException, InvalidRefreshTokenException):
                raise
            except ClientResponseError as exception:
                retry = await self._handle_client_errors(
                    retry=retry, exception=exception
                )
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close()
                retry, retry_seconds = self._get_retry_after(
                    retry=retry, exception=exception
                )
                await self._sleeps.sleep(retry_seconds)

    async def _paginated_api_call(
        self, base_url, breaking_field, is_shared=False, **kwargs
    ):
        """Make a paginated API call for fetching Dropbox files/folders.

        Args:
            base_url (str): Base URL for dropbox APIs
            breaking_field (str): Breaking field to break the pagination
            is_shared (bool): Flag to determine the method flow. If `True` then fetching shared files, else normal files/folders.

        Yields:
            dictionary: JSON response
        """
        data = kwargs["data"]
        url_name = kwargs["url_name"]
        while True:
            try:
                async for response in self.api_call(
                    base_url=base_url,
                    url_name=url_name,
                    data=json.dumps(data),
                ):
                    json_response = await response.json()
                    yield json_response

                    # Breaking condition for pagination
                    # breaking_fields:
                    #  - "has_more" for fetching files/folders
                    #  - "cursor" for fetching shared files
                    if not json_response.get(breaking_field):
                        return

                    data = {
                        BreakingField.CURSOR.value: json_response[
                            BreakingField.CURSOR.value
                        ]
                    }
                    if is_shared:
                        url_name = ENDPOINTS.get(
                            EndpointName.RECEIVED_FILES_CONTINUE.value
                        )
                    else:
                        url_name = ENDPOINTS.get(
                            EndpointName.FILES_FOLDERS_CONTINUE.value
                        )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping of fetching files/folders for url: {parse.urljoin(base_url, url_name)}. Exception: {exception}"
                )
                return

    async def ping(self):
        await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.PING.value),
                data=json.dumps(None),
            )
        )

    async def check_path(self):
        return await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.CHECK_PATH.value),
                data=json.dumps({"path": self.path}),
            )
        )

    async def get_files_folders(self, path):
        data = {
            "path": path,
            "recursive": True,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILES_FOLDERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result

    async def get_shared_files(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            is_shared=True,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILES.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def get_received_file_metadata(self, url):
        data = {"url": url}
        async for response in self.api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_METADATA.value),
            data=json.dumps(data),
        ):
            yield response

    async def download_files(self, path):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.DOWNLOAD.value),
            file_type=FILE,
            path=path,
        ):
            yield response

    async def download_shared_file(self, url):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_DOWNLOAD.value),
            file_type=RECEIVED_FILE,
            url=url,
        ):
            yield response

    async def download_paper_files(self, path):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.PAPER_FILE_DOWNLOAD.value),
            file_type=PAPER,
            path=path,
        ):
            yield response


class DropboxDataSource(BaseDataSource):
    """Dropbox"""

    name = "Dropbox"
    service_type = "dropbox"

    def __init__(self, configuration):
        """Setup the connection to the Dropbox

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.dropbox_client = DropboxClient(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    def _set_internal_logger(self):
        self.dropbox_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Dropbox

        Returns:
            dictionary: Default configuration.
        """
        return {
            "path": {
                "label": "Path to fetch files/folders",
                "order": 1,
                "required": False,
                "type": "str",
                "value": "/",
            },
            "app_key": {
                "label": "Dropbox App Key",
                "sensitive": True,
                "order": 2,
                "type": "str",
                "value": "abc#123",
            },
            "app_secret": {
                "label": "Dropbox App Secret",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "abc#123",
            },
            "refresh_token": {
                "label": "Dropbox Refresh Token",
                "sensitive": True,
                "order": 4,
                "type": "str",
                "value": "abc#123",
            },
            "retry_count": {
                "default_value": RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRY_COUNT,
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": MAX_CONCURRENT_DOWNLOADS,
            },
        }

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured path is available in Dropbox."""

        self.configuration.check_valid()
        await self._remote_validation()

    async def _remote_validation(self):
        try:
            if self.dropbox_client.path not in ["", None]:
                await self.dropbox_client.check_path()
        except InvalidPathException:
            raise
        except Exception as exception:
            raise Exception(
                f"Error while validating path: {self.dropbox_client.path}. Error: {exception}"
            ) from exception

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by dropbox

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        await self.dropbox_client.close()

    async def ping(self):
        await self.dropbox_client.ping()
        self._logger.info("Successfully connected to Dropbox")

    async def _convert_file_to_b64(self, attachment_name, document, temp_filename):
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

    async def _get_document_with_content(
        self, attachment, attachment_name, document, is_shared
    ):
        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            if is_shared:
                async for response in self.dropbox_client.download_shared_file(
                    url=attachment["url"],
                ):
                    async for data in response.content.iter_chunked(CHUNK_SIZE):
                        await async_buffer.write(data)
            elif attachment["is_downloadable"]:
                async for response in self.dropbox_client.download_files(
                    path=attachment["path_display"],
                ):
                    async for data in response.content.iter_chunked(CHUNK_SIZE):
                        await async_buffer.write(data)
            elif attachment_name.split(".")[-1] == PAPER:
                async for response in self.dropbox_client.download_paper_files(
                    path=attachment["path_display"],
                ):
                    async for data in response.content.iter_chunked(CHUNK_SIZE):
                        await async_buffer.write(data)
            else:
                self._logger.warning(
                    f"Skipping the file: {attachment_name} since it is not in the downloadable format."
                )
                return

            temp_filename = str(async_buffer.name)

        return await self._convert_file_to_b64(
            attachment_name=attachment_name,
            document=document,
            temp_filename=temp_filename,
        )

    def _pre_checks_for_get_content(
        self, attachment_extension, attachment_name, attachment_size
    ):
        if attachment_extension == "":
            self._logger.debug(
                f"Files without extension are not supported, skipping {attachment_name}."
            )
            return

        elif attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.debug(
                f"Files with the extension {attachment_extension} are not supported, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        return True

    async def get_content(
        self, attachment, is_shared=False, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            attachment (object): Attachment object
            is_shared (boolean, optional): Flag to check if want a content for shared file. Defaults to False.
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
            attachment_name[attachment_name.rfind(".") :]  # noqa
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
            "_timestamp": attachment["server_modified"],
        }

        return await self._get_document_with_content(
            attachment=attachment,
            attachment_name=attachment_name,
            document=document,
            is_shared=is_shared,
        )

    def _adapt_dropbox_doc_to_es_doc(self, response):
        is_file = response.get(".tag") == "file"
        timestamp = response.get("server_modified") if is_file else iso_utc()
        return {
            "_id": response.get("id"),
            "type": FILE if is_file else FOLDER,
            "name": response.get("name"),
            "file_path": response.get("path_display"),
            "size": response.get("size") if is_file else 0,
            "_timestamp": timestamp,
        }

    def _adapt_dropbox_shared_file_doc_to_es_doc(self, response):
        return {
            "_id": response.get("id"),
            "type": FILE,
            "name": response.get("name"),
            "url": response.get("url"),
            "size": response.get("size"),
            "_timestamp": response.get("server_modified"),
        }

    async def _fetch_files_folders(self, path):
        async for response in self.dropbox_client.get_files_folders(path=path):
            for entry in response.get("entries"):
                yield self._adapt_dropbox_doc_to_es_doc(response=entry), entry

    async def _fetch_shared_files(self):
        async for response in self.dropbox_client.get_shared_files():
            for entry in response.get("entries"):
                async for metadata in self.dropbox_client.get_received_file_metadata(
                    url=entry["preview_url"]
                ):
                    json_metadata = await metadata.json()
                    yield self._adapt_dropbox_shared_file_doc_to_es_doc(
                        response=json_metadata
                    ), json_metadata

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for document, attachment in self._fetch_files_folders(
            path=self.dropbox_client.path
        ):
            if document["type"] == FILE:
                yield document, partial(self.get_content, attachment=attachment)
            else:
                yield document, None

        async for document, attachment in self._fetch_shared_files():
            yield document, partial(
                self.get_content, attachment=attachment, is_shared=True
            )
