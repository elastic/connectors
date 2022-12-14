#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" File streamer service.

Provides:

- `AttachmentService` -- watches a dir for files to upload to Elasticsearch
- `FileDrop` -- used by the producer to drop files on disk

"""
# TODO
#
# - harden all requests (catch errors, retries)
# - if a file keep on failing, put it in a failing dir
# - add a max file size
#
import os
import json
import functools
import re

import aiohttp
import aiofiles

from connectors.logger import logger
from connectors.utils import (
    CancellableSleeps,
    get_event_loop,
    ConcurrentTasks,
)
from connectors.services.base import BaseService


SUPPORTED_TEXT_FILETYPE = [".py", ".rst", ".rb", ".sh", ".md", ".txt"]

# 250MB max disk size
ONE_MEG = 1024 * 1024
DEFAULT_MAX_DIR_SIZE = 250

# Program to encode in base64 -- we could compile a SIMD-aware one
# for an extra performance boost
BASE64 = "base64"
SANITIZER = re.compile("[^0-9a-zA-Z]+")


class FileDrop:
    def __init__(self, config):
        self.config = config
        aconfig = self.config["attachments"]
        self.drop_dir = aconfig["drop_dir"]
        self.max_disk_size = (
            aconfig.get("max_disk_size", DEFAULT_MAX_DIR_SIZE) * ONE_MEG
        )
        self.elastic_config = self.config["elasticsearch"]

    def can_drop(self, drop_directory):
        current_size = 0
        for item in os.scandir(drop_directory):
            try:
                if not item.is_file():
                    continue
                current_size += d.stat().st_size
            except Exception:
                pass

        return current_size <= self.max_disk_size

    def _sanitize(self, name):
        return SANITIZER.sub("-", name)

    async def drop_file(self, gen, name, filename, index, doc_id):
        """Writes a file by chunks using the async generator and then a desc file

        When the desc file hits the disk, it'll be picked up by AttachmentService.
        """
        if not self.can_drop(self.drop_dir):
            raise OSError(f"Limit of {self.max_disk_size} bytes reached")

        key = f"{index}-{doc_id}-{self._sanitize(filename)}"
        target = os.path.join(self.drop_dir, key)
        async with aiofiles.open(target, "wb") as f:
            async for chunk in gen:
                await f.write(chunk)

        logger.info(f"Dropped {key}")

        # the file is now on disk, let's create the desc
        desc = {
            # XXX maybe its overkill for v1 since a node only connects to a
            # single ES
            # XXX add suport for API key
            "filename": key,
            "index": index,
            # XXX how do we know this value when the initial sync of the doc is
            # not done yet. we might need to use the source id
            # and at sync time, query for the corresponding ES doc id
            # and send it only once it's there..
            "doc_id": doc_id,
            "name": filename,
        }

        desc_file = os.path.join(self.drop_dir, os.path.splitext(key)[0] + ".json")
        with open(desc_file, "w") as f:
            f.write(json.dumps(desc))

        logger.info(f"Dropped {desc_file}")


class AttachmentService(BaseService):
    """Watches a directory for files and sends them by chunks

    Uses `Transfer-Encoding: chunked`
    """

    def __init__(self, args):
        super().__init__(args)
        self.elastic_config = self.config["elasticsearch"]
        self._config = self.config.get("attachments", {})
        self.max_concurrency = self._config.get("max_concurrency", 5)
        self.directory = self._prepare_dir("drop")
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
        cmd = f"{BASE64} {filename} > {filename}.b64"
        logger.debug(f"[{os.path.basename(filename)}] Running {cmd}")
        os.system(cmd)
        return f"{filename}.b64"

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
                chunk = (await f.read(chunk_size)).strip()
                while chunk:
                    yield chunk
                    chunk = (await f.read(chunk_size)).strip()
                yield b'"}'
        finally:
            if os.path.exists(b64_file):
                os.remove(b64_file)

    def _prepare_dir(self, name):
        default_dir = os.path.join(os.path.dirname(self.config_file), "attachments")
        path = self._config.get(f"{name}_dir", os.path.join(default_dir, name))
        os.makedirs(path, exist_ok=True)
        return path

    async def _send_attachment(self, desc_file, desc, session):
        fn = desc["filename"]
        filename = os.path.join(self.directory, fn)
        gen = functools.partial(self._file_to_pipeline, filename)
        url = f"{self.elastic_config['host']}/{desc['index']}/_doc/{desc['doc_id']}?pipeline=attachment"
        logger.debug(f"[{fn}] Sending by chunks to {url}")
        headers = {"Content-Type": "application/json"}
        result = os.path.join(self.results_directory, fn + ".json")
        worked = False
        resp = None

        async with session.put(url, data=gen(), headers=headers) as resp:
            async with aiofiles.open(result, "w") as f:
                resp = await resp.json()
                logger.debug(f"[{fn}] Done, results in {result}")
                if resp.get("result") in ("updated", "created"):
                    logger.debug(f"document was {resp['result']}")
                    worked = True
                await f.write(json.dumps(resp))

        return worked, filename, desc_file, resp

    async def _process_dir(self):
        logger.info("Scanning now...")

        sent = [0]

        def track_results(result):
            worked, filename, desc_file, resp = result
            if worked:
                os.remove(filename)
                os.remove(desc_file)
                sent[0] += 1
                if sent[0] % 10 == 0:
                    logger.info(f"Sent {sent[0]} files.")
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

            for file in os.listdir(self.directory):
                if not file.endswith(".json"):
                    continue
                desc_file = os.path.join(self.directory, file)
                async with aiofiles.open(desc_file) as f:
                    desc = json.loads(await f.read())
                    logger.debug(f"Processing {desc['name']}")
                    await runner.put(
                        functools.partial(
                            self._send_attachment, desc_file, desc, session
                        )
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
