#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Demo of a standalone source
"""
import os
from pathlib import Path
import hashlib
from datetime import datetime, timezone

import aiofiles
from connectors.source import BaseDataSource
from connectors.utils import logger


HERE = os.path.dirname(__file__)


class DirectoryDataSource(BaseDataSource):
    """Directory"""

    def __init__(self, connector):
        super().__init__(connector)
        self.directory = connector.configuration["directory"]
        self.pattern = connector.configuration["pattern"]
        self.max_file_size = int(
            connector.configuration.get("max_file_size", 20 * 1024 * 1024)
        )

    @classmethod
    def get_default_configuration(cls):
        return {
            "directory": {
                "value": "/Users/tarekziade/Library/Mobile Documents",
                "label": "Directory",
                "type": "str",
            },
            "max_file_size": {
                "value": 20 * 1024 * 1024,
                "label": "Maximum file size",
                "type": "int",
            },
            "pattern": {
                "value": "**/*.*",
                "label": "File glob-like pattern",
                "type": "str",
            },
        }

    async def ping(self):
        return True

    async def changed(self):
        return True

    def get_id(self, path):
        return hashlib.md5(str(path).encode("utf8")).hexdigest()

    async def close(self):
        pass

    async def _download(self, path):
        chunk_size = 1024 * 128
        print(f"Reading {path}")
        async with aiofiles.open(path, "rb") as f:
            chunk = (await f.read(chunk_size)).strip()
            while chunk:
                yield chunk
                chunk = (await f.read(chunk_size)).strip()

    async def get_docs(self):
        root_directory = Path(self.directory)
        for path_object in root_directory.glob(self.pattern):
            if not path_object.is_file():
                continue

            fstat = path_object.stat()

            # get the file size
            if fstat.st_size >= self.max_file_size:
                logger.debug(f"Discard large file {str(path_object)}")
                continue

            # get the last modified value of the file
            ts = fstat.st_mtime
            ts = datetime.fromtimestamp(ts, tz=timezone.utc)

            # send back as a doc
            doc = {
                "path": str(path_object),
                "timestamp": ts.isoformat(),
                "_id": self.get_id(path_object),
                "_attachment": self._download(str(path_object)),
                "_attachment_name": path_object.name,
                "_attachment_filename": path_object.name,
            }

            yield doc, None
