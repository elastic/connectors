#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
from functools import cached_property, partial

import aiofiles
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from connectors_sdk.content_extraction import TIKA_SUPPORTED_FILETYPES
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import convert_to_b64

from connectors.sources.box.client import BoxClient
from connectors.sources.box.utils import (
    BOX_ENTERPRISE,
    BOX_FREE,
    CHUNK_SIZE,
    ENDPOINTS,
    FIELDS,
    FILE,
    FINISHED,
    MAX_CONCURRENCY,
    MAX_CONCURRENT_DOWNLOADS,
    QUEUE_MEM_SIZE,
)
from connectors.utils import ConcurrentTasks, MemQueue


class BoxDataSource(BaseDataSource):
    name = "Box"
    service_type = "box"
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.tasks = 0
        self.is_enterprise = configuration["is_enterprise"]
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.concurrent_downloads = configuration["concurrent_downloads"]

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
        options["concurrent_downloads"] = self.concurrent_downloads

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Box.

        Returns:
            dict: Default configuration.
        """
        return {
            "is_enterprise": {
                "display": "dropdown",
                "label": "Box Account",
                "options": [
                    {"label": "Box Free Account", "value": BOX_FREE},
                    {"label": "Box Enterprise Account", "value": BOX_ENTERPRISE},
                ],
                "order": 1,
                "type": "str",
                "value": BOX_FREE,
            },
            "client_id": {
                "label": "Client ID",
                "order": 2,
                "type": "str",
            },
            "client_secret": {
                "label": "Client Secret",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "refresh_token": {
                "depends_on": [{"field": "is_enterprise", "value": BOX_FREE}],
                "label": "Refresh Token",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "enterprise_id": {
                "depends_on": [{"field": "is_enterprise", "value": BOX_ENTERPRISE}],
                "label": "Enterprise ID",
                "order": 5,
                "type": "int",
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
            },
        }

    async def close(self):
        while not self.queue.empty():
            await self.queue.get()
        await self.client.close()

    async def ping(self):
        try:
            await self.client.ping()
            self._logger.debug("Successfully connected to Box")
        except Exception:
            self._logger.warning("Error while connecting to Box")
            raise

    async def get_users_id(self):
        self._logger.debug("Fetching users")
        async for user in self.client.paginated_call(
            url=ENDPOINTS["USERS"], params={}, headers={}
        ):
            yield user.get("id")

    async def _fetch(self, doc_id, user_id=None):
        self._logger.info(
            f"Fetching files and folders recursively for folder ID: {doc_id}"
        )
        try:
            params = {
                "fields": FIELDS,
            }
            async for folder_entry in self.client.paginated_call(
                url=ENDPOINTS["FOLDER"].format(folder_id=doc_id),
                params=params,
                headers={"as-user": user_id} if user_id else {},
            ):
                doc = folder_entry.copy()
                doc["_id"] = doc.pop("id")
                doc["_timestamp"] = doc.pop("modified_at")
                if folder_entry.get("type") == FILE:
                    await self.queue.put(
                        (
                            doc,
                            partial(
                                self.get_content,
                                attachment=folder_entry,
                                user_id=user_id,
                            ),
                        )
                    )
                elif folder_entry.get("type") == "folder":
                    await self.queue.put((doc, None))
                    self.tasks += 1
                    await self.fetchers.put(
                        partial(
                            self._fetch,
                            doc_id=folder_entry.get("id"),
                            user_id=user_id,
                        )
                    )
        except Exception as exception:
            self._logger.info(
                f"Something went wrong while fetching data from the folder ID: {doc_id}. Error: {exception}"
            )
            raise
        finally:
            await self.queue.put(FINISHED)

    async def _get_document_with_content(self, url, attachment_name, document, user_id):
        file_data = await self.client.get(
            url=url, headers={"as-user": user_id} if user_id else {}
        )
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

        if attachment_size > self.framework_config.max_file_size:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {self.framework_config.max_file_size} bytes. Discarding file content"
            )
            return False
        return True

    async def get_content(self, attachment, user_id=None, timestamp=None, doit=False):
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
            user_id=user_id,
        )

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Box.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()
            if item == FINISHED:
                self.tasks -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        seen_ids = set()
        root_folder = "0"
        if self.is_enterprise == BOX_ENTERPRISE:
            self._logger.info("Fetching data from Box's Enterprise Account")
            async for user_id in self.get_users_id():
                # "0" refers to the root folder
                await self.fetchers.put(
                    partial(self._fetch, doc_id=root_folder, user_id=user_id)
                )
                self.tasks += 1
        else:
            self._logger.info("Fetching data from Box's Free Account")
            await self.fetchers.put(partial(self._fetch, doc_id=root_folder))
            self.tasks += 1

        async for item in self._consumer():
            current_id = item[0].get("_id")
            if current_id in seen_ids:
                self._logger.debug(
                    f"Already processed item with id '{current_id}'. Skipping item..."
                )
                continue
            else:
                seen_ids.add(current_id)
                yield item

        await self.fetchers.join()
