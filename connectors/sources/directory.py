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
from connectors.utils import get_base64_value, TIKA_SUPPORTED_FILETYPES


HERE = os.path.dirname(__file__)
DEFAULT_CONTENT_EXTRACTION = True

class DirectoryDataSource:
    """Directory"""

    def __init__(self, connector):
        self.directory = connector.configuration["directory"]
        self.pattern = connector.configuration["pattern"]
        self.enable_content_extraction = connector.configuration.get(
            "enable_content_extraction", DEFAULT_CONTENT_EXTRACTION
        )

    @classmethod
    def get_default_configuration(cls):
        return {
            "directory": {
                "value": HERE,
                "label": "Directory",
                "type": "str",
            },
            "pattern": {
                "value": "**/*.py",
                "label": "File glob-like pattern",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Flag to check if content extraction is enabled or not",
                "type": "bool",
            },
        }

    async def ping(self):
        return True

    async def changed(self):
        return True

    def get_id(self, path):
        return hashlib.md5(str(path).encode("utf8")).hexdigest()

    async def _download(self, path, timestamp=None, doit=None):
        if not (
            self.enable_content_extraction
            and doit
            and os.path.splitext(path)[-1] in TIKA_SUPPORTED_FILETYPES
        ):
            return

        print(f"Reading {path}")
        with open(file=path, mode="rb") as f:
            return {
                "_id": self.get_id(path),
                "timestamp": timestamp,
                "_attachment": get_base64_value(f.read()),
            }

    async def get_docs(self):
        root_directory = Path(self.directory)
        for path_object in root_directory.glob(self.pattern):
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
