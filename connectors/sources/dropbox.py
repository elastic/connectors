#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online.
"""
import asyncio
import os
from functools import cached_property, partial

import aiofiles
import dropbox
import requests
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from dropbox.exceptions import ApiError, AuthError, BadInputError
from dropbox.files import FileMetadata
from dropbox.sharing import FileLinkMetadata, SharedFileMetadata

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    convert_to_b64,
    iso_utc,
)

RETRY_COUNT = 3
FILE_SIZE_LIMIT = 10485760  # Size in Bytes
MAX_CONCURRENT_DOWNLOADS = 100
LIMIT = 300  # Limit for fetching shared files per call
PAPER = "paper"
FILE = "File"
FOLDER = "Folder"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


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
        self._session = None

    @cached_property
    def _connection(self):
        self._session = requests.Session()
        return dropbox.Dropbox(  # pyright: ignore
            app_key=self.configuration["app_key"],
            app_secret=self.configuration["app_secret"],
            oauth2_refresh_token=self.configuration["refresh_token"],
            max_retries_on_error=self.retry_count,
            max_retries_on_rate_limit=self.retry_count,
            session=self._session,
        )

    def ping(self):
        self._connection.users_get_current_account()

    def check_path(self):
        return self._connection.files_get_metadata(path=self.path)

    async def _pagination_wrapper(self, response, break_property, pagination_call):
        while True:
            for entry in response.entries:  # pyright: ignore
                yield entry
            if not getattr(response, break_property):
                break
            response = await asyncio.to_thread(
                partial(
                    pagination_call,
                    cursor=response.cursor,  # pyright: ignore
                )
            )

    async def get_files_folders(self, path):
        response = await asyncio.to_thread(
            partial(
                self._connection.files_list_folder,
                path=path,
                recursive=True,
            )
        )
        async for result in self._pagination_wrapper(
            response=response,
            break_property="has_more",
            pagination_call=self._connection.files_list_folder_continue,
        ):
            yield result

    async def get_shared_files(self):
        shared_files = await asyncio.to_thread(
            self._connection.sharing_list_received_files, limit=LIMIT
        )
        async for result in self._pagination_wrapper(
            response=shared_files,
            break_property="cursor",
            pagination_call=self._connection.sharing_list_received_files_continue,
        ):
            yield result

    def download_files(self, path):
        return self._connection.files_download(path=path)

    def download_paper_files(self, path):
        return self._connection.files_export(path=path, export_format="markdown")

    def download_shared_files(self, url):
        return self._connection.sharing_get_shared_link_file(url=url)

    def close(self):
        self._sleeps.cancel()
        if self._session is not None:
            self._connection.close()
            del self._session


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
                "default_value": "/",
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
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
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
                self.dropbox_client.check_path()
        except BadInputError as err:
            raise ConfigurableFieldValueError(
                "Configured App Key or App Secret is invalid"
            ) from err
        except AuthError as err:
            raise ConfigurableFieldValueError(
                "Configured Refresh Token is invalid"
            ) from err
        except ApiError as err:
            if err.error.is_path() and err.error.get_path().is_not_found():
                raise ConfigurableFieldValueError(
                    f"Configured Path: {self.dropbox_client.path} is invalid"
                ) from err
            else:
                raise Exception(
                    f"Error while validating the configured path. Error: {err}"
                ) from err
        except Exception as exception:
            raise Exception(f"Something went wrong. Error: {exception}") from exception

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by dropbox

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        self.dropbox_client.close()

    async def ping(self):
        try:
            self.dropbox_client.ping()
            logger.debug("Successfully connected to Dropbox")
        except BadInputError as err:
            raise ConfigurableFieldValueError(
                "Configured App Key or App Secret is invalid"
            ) from err
        except AuthError as err:
            raise ConfigurableFieldValueError(
                "Configured Refresh Token is invalid"
            ) from err
        except Exception:
            raise

    async def _get_b64_content(self, attachment_name, temp_filename, document):
        logger.debug(f"Calling convert_to_b64 for file : {attachment_name}")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)
        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            # base64 on macOS will add a EOL, so we strip() here
            content = (await async_buffer.read()).strip()
        try:
            await remove(temp_filename)
        except Exception as exception:
            logger.warning(
                f"Could not remove file from: {temp_filename}. Error: {exception}"
            )
        return content

    async def _get_document_with_content(
        self, attachment, attachment_name, document, shared_file_response
    ):
        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            if isinstance(attachment, FileLinkMetadata):
                await async_buffer.write(shared_file_response.content)
            elif attachment.is_downloadable:
                _, data = self.dropbox_client.download_files(  # pyright: ignore
                    path=attachment.path_display
                )
                await async_buffer.write(data.content)
            elif attachment_name.split(".")[-1] == PAPER:
                _, data = self.dropbox_client.download_paper_files(  # pyright: ignore
                    path=attachment.path_display
                )
                await async_buffer.write(data.content)
            else:
                logger.warning(
                    f"Skipping the file: {attachment_name} since it is not in the downloadable format."
                )
                return

            temp_filename = str(async_buffer.name)

        document["_attachment"] = await self._get_b64_content(
            attachment_name=attachment_name,
            temp_filename=temp_filename,
            document=document,
        )

        return document

    async def get_content(self, attachment, response=None, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            response (object): Response of a shared file
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        attachment_size = int(attachment.size)
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment.name
        attachment_extension = os.path.splitext(attachment_name)[-1]

        if attachment_extension == "":
            logger.warning(
                f"Files without extension are not supported by TIKA, skipping {attachment_name}."
            )
            return

        elif attachment_extension not in TIKA_SUPPORTED_FILETYPES:
            logger.warning(
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": attachment.id,
            "_timestamp": attachment.server_modified,
        }

        return await self._get_document_with_content(
            attachment=attachment,
            attachment_name=attachment_name,
            document=document,
            shared_file_response=response,
        )

    def _prepare_doc(self, response, shared_metadata=None):
        if isinstance(response, SharedFileMetadata):
            return {
                "_id": response.id,
                "type": FILE,
                "name": response.name,
                "file path": response.path_display,
                "url": response.preview_url,
                "size": shared_metadata.size,
                "_timestamp": response.time_invited.strftime(DATETIME_FORMAT),
            }

        is_file = isinstance(response, FileMetadata)
        timestamp = (
            response.server_modified.strftime(DATETIME_FORMAT) if is_file else iso_utc()
        )
        return {
            "_id": response.id,
            "type": FILE if is_file else FOLDER,
            "name": response.name,
            "file path": response.path_display,
            "size": response.size if is_file else 0,
            "_timestamp": timestamp,
        }

    async def _fetch_files_folders(self):
        async for response in self.dropbox_client.get_files_folders(
            path=self.dropbox_client.path
        ):
            yield response, self._prepare_doc(response=response)

    async def _fetch_shared_files(self):
        async for response in self.dropbox_client.get_shared_files():
            (
                metadata,
                shared_response,
            ) = self.dropbox_client.download_shared_files(  # pyright: ignore
                url=response.preview_url
            )
            yield metadata, shared_response, self._prepare_doc(
                response=response, shared_metadata=metadata
            )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for response, document in self._fetch_files_folders():
            if isinstance(response, FileMetadata):
                yield document, partial(self.get_content, attachment=response)
            else:
                yield document, None

        async for metadata, shared_response, document in self._fetch_shared_files():
            yield document, partial(
                self.get_content, attachment=metadata, response=shared_response
            )
