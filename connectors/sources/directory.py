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
import functools
from datetime import datetime, timezone


HERE = os.path.dirname(__file__)


class DirectoryDataSource:
    def __init__(self, connector):
        self.directory = connector.configuration["directory"]

    @classmethod
    def get_default_configuration(cls):
        return {
            "directory": {
                "value": HERE,
                "label": "Directory",
                "type": "str",
            },
        }

    async def ping(self):
        return True

    async def changed(self):
        return True

    def get_id(self, path):
        return hashlib.md5(str(path).encode("utf8")).hexdigest()

    async def _download(self, path, timestamp=None, doit=None):
        if not doit:
            return

        print(f"Reading {path}")
        with open(path) as f:
            return {"_id": self.get_id(path), "timestamp": timestamp, "text": f.read()}

    async def get_docs(self):
        root_directory = Path(self.directory)
        for path_object in root_directory.glob("**/*.py"):
            if not path_object.is_file():
                continue

            # download coroutine
            download_coro = functools.partial(self._download, str(path_object))

            # get the last modified value of the file
            ts = path_object.stat().st_mtime
            ts = datetime.fromtimestamp(ts, tz=timezone.utc)

            # send back as a doc
            doc = {
                "path": str(path_object),
                "timestamp": ts.isoformat(),
                "_id": self.get_id(path_object),
            }

            yield doc, download_coro
