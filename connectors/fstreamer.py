#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
""" File streamer service.

Provides:

- `FileUploadService` -- watches a dir for files to upload to Elasticsearch
- `can_drop()` -- returns True is there's space to drop a file
- `drop_file()` -- used by the producer to drop files on disk

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

import asyncio
import aiohttp
import aiofiles

#
# using some connectors stuff since we'll end up there
#
from connectors.logger import logger
from connectors.utils import (CancellableSleeps, Service, get_event_loop,
                              ConcurrentRunner)

# 250MB max disk size
MAX_DIR_SIZE = 250 * 1024 * 1024
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


def can_drop(drop_directory):
    current_size = sum(
        d.stat().st_size for d in os.scandir(drop_directory) if d.is_file()
    )
    return current_size <= MAX_DIR_SIZE


async def drop_file(
    gen, drop_directory, name, filename, index, doc_id, host, user, password
):
    """Writes a file by chunks using the async generator and then a desc file

    Once the desc file hit the disk, it'll be picked up by FileUploadService.
    """
    if not can_drop(drop_directory):
        raise OSError("Limit reached")

    target = os.path.join(drop_directory, filename)
    async with aiofiles.open(target, "wb") as f:
        async for chunk in gen():
            f.write(chunk)

    logger.info(f"Dropped {filename}")

    # the file is now on disk, let's create the desc
    desc = {
        "host": host,
        "user": user,
        "password": password,
        "filename": filename,
        "index": index,
        "doc_id": doc_id,
        "name": name,
    }
    desc_file = os.path.join(drop_directory, os.path.splitext(0) + ".json")
    with open(desc_file, "w") as f:
        f.write(json.dumps(desc))

    logger.info(f"Dropped {desc_file}")


class FileUploadService(Service):
    """Watches a directory for files and send them."""

    def __init__(self, args):
        self.directory = args.directory
        self.idle_time = args.idle_time
        self.running = False
        self.log_directory = args.log_directory
        self._sleeps = CancellableSleeps()

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
        result = os.path.join(self.log_directory, fn + ".json")
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

        runner = ConcurrentRunner(results_cb=track_results)
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
