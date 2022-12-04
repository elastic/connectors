#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# TODO
#
# - harden all requests (catch errors, retries)
# - if a file keep on failing, put it in a failing dir
# - add a max file size
#
import functools
import json
import os
import re

import aiofiles
import aiohttp

from connectors.logger import logger
from connectors.services.base import BaseService
from connectors.services.fstreamer.dropper import UploadDir
from connectors.utils import (
    CancellableSleeps,
    ConcurrentTasks,
    convert_to_b64,
    get_event_loop,
)

SUPPORTED_TEXT_FILETYPE = [".py", ".rst", ".rb", ".sh", ".md", ".txt"]

# 250MB max disk size
ONE_MEG = 1024 * 1024
DEFAULT_MAX_DIR_SIZE = 250

# Program to encode in base64 -- we could compile a SIMD-aware one
# for an extra performance boost
BASE64 = "base64"
SANITIZER = re.compile("[^0-9a-zA-Z]+")


class FileUploadService(BaseService):
    """Watches a directory for files and sends them by chunks

    Uses `Transfer-Encoding: chunked`
    """

    name = "streamer"

    def __init__(self, args):
        super().__init__(args)
        self.elastic_config = self.config["elasticsearch"]
        self._config = self.config.get("attachments", {})
        self.max_concurrency = self._config.get("max_concurrency", 5)
        self.directory = UploadDir(self._prepare_dir("drop"))
        self.idle_time = self._config.get("idling", 30)
        self.log_directory = self._prepare_dir("logs")
        self.results_directory = self._prepare_dir("results")
        self.running = False
        self._sleeps = CancellableSleeps()

    def _to_b64(self, filename):
        """Calls the system base64 utility to create a base64-encoded file

        The base64 utility is a stream encoder, the file will not be fully
        loaded into memory.

        This is a blocking method.
        """
        return convert_to_b64(filename)

    async def _file_to_pipeline(self, filename, chunk_size=1024 * 1024):
        """Convert a file into a streamable Elasticsearch request.

        - Calls `_to_b64` in a separate thread so we don't block
        - Reads the base64 file by chunks an provide an async generator
        """
        b64_file = await get_event_loop().run_in_executor(None, self._to_b64, filename)

        # XXX adding .txt to all known text files
        if os.path.splitext(filename)[-1] in SUPPORTED_TEXT_FILETYPE:
            filename += ".txt"

        try:
            async with aiofiles.open(b64_file, "rb") as f:
                yield b'{"filename":"' + filename.encode("utf8") + b'","data":"'
                while chunk := (await f.read(chunk_size)).strip():
                    yield chunk
                yield b'"}'
        finally:
            if os.path.exists(b64_file):
                os.remove(b64_file)

    def _prepare_dir(self, name):
        default_dir = os.path.join(os.getcwd(), "attachments")
        path = self._config.get(f"{name}_dir", os.path.join(default_dir, name))
        os.makedirs(path, exist_ok=True)
        return path

    async def _send_attachment(self, metadata, session):
        filename = metadata.target_filename
        name = metadata.name
        # single host but we could target multiple
        gen = functools.partial(self._file_to_pipeline, filename)
        host = metadata.target_elastic["host"]
        url = (
            f"{host}/{metadata.target_index}/_doc/{metadata.doc_id}?pipeline=attachment"
        )
        logger.debug(f"[{name}] Sending by chunks to {url}")
        headers = {"Content-Type": "application/json"}
        result = os.path.join(self.results_directory, filename + ".json")
        worked = False
        resp = None

        async with session.put(url, data=gen(), headers=headers) as resp:
            async with aiofiles.open(result, "w") as f:
                resp = await resp.json()
                logger.debug(f"[{filename}] Done, results in {result}")
                if resp.get("result") in ("updated", "created"):
                    logger.debug(f"document was {resp['result']}")
                    worked = True
                await f.write(json.dumps(resp))

        return worked, filename, metadata, resp

    async def _process_dir(self):
        logger.info("Scanning now...")

        sent = 0

        def track_results(result):
            nonlocal sent

            worked, filename, desc_file, resp = result
            if worked:
                os.remove(filename)
                os.remove(desc_file)
                sent += 1
                if sent % 10 == 0:
                    logger.info(f"Sent {sent} files.")
            else:
                logger.error(f"Failed to ingest {filename}")
                logger.error(json.dumps(resp))

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(
                self.elastic_config["username"], self.elastic_config["password"]
            )
        ) as session:
            runner = ConcurrentTasks(
                max_concurrency=self.max_concurrency, results_callback=track_results
            )

            async for metadata in self.directory.get_files():
                logger.debug(f"Processing {metadata.name}")
                await runner.put(
                    functools.partial(self._send_attachment, metadata, session)
                )

            await runner.join()

    #
    # public APIS
    #
    async def run(self):
        self.running = True
        logger.info(f"Watching {self.directory}")
        while self.running:
            await self._process_dir()
            logger.info(f"Sleeping for {self.idle_time}s")
            await self._sleeps.sleep(self.idle_time)

    def shutdown(self, *args, **kw):
        self.running = False
        self._sleeps.cancel()
