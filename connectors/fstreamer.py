#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" File streamer service.

Provides:

- `FileUploadService` -- watches a dir for files to upload to Elasticsearch
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

import aiohttp
import aiofiles

#
# using some connectors stuff since we'll end up there
#
from connectors.logger import logger
from connectors.utils import (
    CancellableSleeps,
    Service,
    get_event_loop,
    ConcurrentRunner,
)

# 250MB max disk size
ONE_MEG = 104 * 1024
DEFAULT_MAX_DIR_SIZE = 250
BASE64 = "base64"


def to_b64(filename):
    cmd = f"{BASE64} {filename} > {filename}.b64"
    logger.info(f"[{os.path.basename(filename)}] Running {cmd}")
    os.system(cmd)
    return f"{filename}.b64"


async def file_to_pipeline(filename, chunk_size=1024 * 128):
    """Convert a file into a streamable Elasticsearch request."""
    b64_file = await get_event_loop().run_in_executor(None, to_b64, filename)

    try:
        async with aiofiles.open(filename, "rb") as f:
            yield b'{"data":"'
            chunk = (await f.read(chunk_size)).strip()
            while chunk:
                yield chunk
                chunk = (await f.read(chunk_size)).strip()
            yield b'"}'
    finally:
        if os.path.exists(b64_file):
            os.remove(b64_file)


class FileDrop:
    def __init__(self, config):
        self.config = config
        self.drop_dir = self.config["attachments"]["drop_dir"]
        self.max_disk_size = self.config["attachments"]["max_disk_size"] * ONE_MEG
        self.elastic_config = self.config["elastic_search"]

    def can_drop(self, drop_directory):
        current_size = sum(
            d.stat().st_size for d in os.scandir(drop_directory) if d.is_file()
        )
        return current_size <= self.max_disk_size

    async def drop_file(self, gen, name, filename, index, doc_id):
        """Writes a file by chunks using the async generator and then a desc file

        When the desc file hits the disk, it'll be picked up by FileUploadService.
        """
        if not self.can_drop(self.drop_dir):
            raise OSError("Limit reached")

        target = os.path.join(self.drop_dir, filename)
        async with aiofiles.open(target, "wb") as f:
            async for chunk in gen():
                f.write(chunk)

        logger.info(f"Dropped {filename}")

        # the file is now on disk, let's create the desc
        desc = {
            "host": self.elastic_config["host"],
            # XXX add suport for API key
            "user": self.elastic_config["user"],
            "password": self.elastic_config["password"],
            "filename": filename,
            "index": index,
            "doc_id": doc_id,
            "name": name,
        }

        desc_file = os.path.join(self.drop_dir, os.path.splitext(0) + ".json")
        with open(desc_file, "w") as f:
            f.write(json.dumps(desc))

        logger.info(f"Dropped {desc_file}")


class FileUploadService(Service):
    """Watches a directory for files and sends them by chunks

    Uses `Transfer-Encoding: chunked`
    """

    def __init__(self, args):
        super().__init__(args)
        self._config = self.config["attachments"]
        self.max_concurrency = self._config["max_concurrency"]
        self.directory = self._prepare_dir("drop")
        self.idle_time = self._config.get("idling", 30)
        self.log_directory = self._prepare_dir("logs")
        self.results_directory = self._prepare_dir("results")
        self.running = False
        self._sleeps = CancellableSleeps()

    def _prepare_dir(self, name):
        default_dir = os.path.join(os.path.dirname(self.config_file), "attachments")
        path = self._config.get(f"{name}_dir", os.path.join(default_dir, name))
        os.makedirs(path, exist_ok=True)
        return path

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

    async def send_attachment(self, desc_file, desc):
        fn = desc["filename"]

        filename = os.path.join(self.directory, fn)
        gen = functools.partial(file_to_pipeline, filename)
        url = (
            f"{desc['host']}/{desc['index']}/_doc/{desc['doc_id']}?pipeline=attachment"
        )
        logger.info(f"[{fn}] Sending by chunks to {url}")
        headers = {"Content-Type": "application/json"}
        result = os.path.join(self.results_directory, fn + ".json")
        worked = False
        resp = None

        async with aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(desc["user"], desc["password"])
        ) as session:
            async with session.put(url, data=gen(), headers=headers) as resp:
                async with aiofiles.open(result, "w") as f:
                    resp = await resp.json()
                    logger.info(f"[{fn}] Done, results in {result}")
                    if resp.get("result") in ("updated", "created"):
                        logger.info(f"document was {resp['result']}")
                        worked = True
                    await f.write(json.dumps(resp))

        return worked, filename, desc_file, resp

    async def _process_dir(self):
        logger.info("Scanning now...")

        def track_results(result):
            worked, filename, desc_file, resp = result
            if worked:
                os.remove(filename)
                os.remove(desc_file)
            else:
                logger.error(f"Failed to ingest {filename}")
                logger.error(json.dumps(resp))

        runner = ConcurrentRunner(
            max_concurrency=self.max_concurrency, results_cb=track_results
        )

        for file in os.listdir(self.directory):
            if not file.endswith(".json"):
                continue
            desc_file = os.path.join(self.directory, file)
            async with aiofiles.open(desc_file) as f:
                desc = json.loads(await f.read())
                logger.info(f"Processing {desc['name']}")
                await runner.put(
                    functools.partial(self.send_attachment, desc_file, desc)
                )

        await runner.wait()
