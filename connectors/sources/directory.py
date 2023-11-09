#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Demo of a standalone source
"""
import functools
import os
from datetime import datetime, timezone
from pathlib import Path

import aiofiles

from connectors.source import BaseDataSource
from connectors.utils import TIKA_SUPPORTED_FILETYPES, get_base64_value, hash_id

DEFAULT_DIR = os.environ.get("SYSTEM_DIR", os.path.dirname(__file__))


class DirectoryDataSource(BaseDataSource):
    """Directory"""

    name = "System Directory"
    service_type = "dir"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.directory = os.path.abspath(self.configuration["directory"])
        self.pattern = self.configuration["pattern"]

    @classmethod
    def get_default_configuration(cls):
        return {
            "directory": {
                "label": "Directory path",
                "order": 1,
                "type": "str",
                "validations": [],
                "value": DEFAULT_DIR,
            },
            "pattern": {
                "display": "text",
                "label": "File glob-like pattern",
                "order": 2,
                "type": "str",
                "value": "**/*.*",
            },
        }

    async def ping(self):
        return True

    async def changed(self):
        return True

    def get_id(self, path):
        return hash_id(str(path))

    async def _download(self, path, timestamp=None, doit=None):
        if not (doit and os.path.splitext(path)[-1] in TIKA_SUPPORTED_FILETYPES):
            return

        self._logger.info(f"Reading {path}")
        async with aiofiles.open(file=path, mode="rb") as f:
            content = await f.read()
            return {
                "_id": self.get_id(path),
                "_timestamp": timestamp,
                "_attachment": get_base64_value(content),
            }

    async def get_docs(self, filtering=None):
        self._logger.debug(f"Reading {self.directory}...")
        root_directory = Path(self.directory)

        for path_object in root_directory.glob(self.pattern):
            if not path_object.is_file():
                continue

            # download coroutine
            download_coro = functools.partial(self._download, str(path_object))

            # get the last modified value of the file
            stat = path_object.stat()
            ts = stat.st_mtime
            ts = datetime.fromtimestamp(ts, tz=timezone.utc)

            # send back as a doc
            doc = {
                "path": str(path_object),
                "last_modified_time": ts,
                "inode_protection_mode": stat.st_mode,
                "inode_number": stat.st_ino,
                "device_inode_reside": stat.st_dev,
                "number_of_links": stat.st_nlink,
                "uid": stat.st_uid,
                "gid": stat.st_gid,
                "ctime": stat.st_ctime,
                "last_access_time": stat.st_atime,
                "size": stat.st_size,
                "_timestamp": ts.isoformat(),
                "_id": self.get_id(path_object),
            }

            yield doc, download_coro
