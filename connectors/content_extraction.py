#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientConnectionError, ServerTimeoutError

from connectors.logger import logger


class ContentExtraction:
    """Content extraction manager

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

        self.extraction_config = ContentExtraction.get_extraction_config()
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
