import json
import os
import re

import aiofiles
from aiofiles.os import listdir  # type: ignore

SANITIZER = re.compile("[^0-9a-zA-Z]+")


# 250MB max disk size
ONE_MEG = 1024 * 1024
DEFAULT_MAX_DIR_SIZE = 250


class FileMetadata:
    def __init__(self, name, target_elastic, target_index, doc_id, filename):
        self.target_elastic = target_elastic
        self.target_index = target_index
        self.doc_id = doc_id
        self.target_filename = filename
        self.drop_dir = os.path.dirname(self.target_filename)
        self.filename = os.path.join(
            self.drop_dir, os.path.splitext(self.target_filename)[0] + ".metadata"
        )
        self.name = name

    async def write(self):
        desc = {
            "name": self.name,
            "filename": self.target_filename,
            "target_index": self.target_index,
            "target_elastic": self.target_elastic,
            # XXX how do we know this value when the initial sync of the doc is
            # not done yet. we might need to use the source id
            # and at sync time, query for the corresponding ES doc id
            # and send it only once it's there..
            "doc_id": self.doc_id,
        }

        async with aiofiles.open(self.filename, "w") as f:
            await f.write(json.dumps(desc))

    @classmethod
    async def read(cls, filename):
        async with aiofiles.open(filename, "r") as f:
            data = json.loads(await f.read())

        return cls(
            data["name"],
            data["target_elastic"],
            data["target_index"],
            data["doc_id"],
            data["filename"],
        )


class UploadDir:
    """Represents the upload dir"""

    def __init__(self, drop_dir, max_disk_size=DEFAULT_MAX_DIR_SIZE * ONE_MEG):
        self.drop_dir = drop_dir
        self.max_disk_size = max_disk_size
        # initial scan to get the size
        self.current_size = 0
        for item in os.scandir(self.drop_dir):
            try:
                if not item.is_file():
                    continue
                self.current_size += item.stat().st_size
            except Exception:
                pass

    def __str__(self):
        return f"<UploadDir {self.drop_dir} {self.current_size}/{self.max_disk_size}>"

    def _sanitize(self, name):
        return SANITIZER.sub("-", name)

    async def drop_file(self, gen, target_elastic, name, filename, index, doc_id):
        """Writes a file by chunks using the async generator and then a metadata file"""

        if self.current_size >= self.max_disk_size:
            raise OSError(
                f"Limit of {self.max_disk_size} bytes reached: {self.current_size}"
            )

        key = f"{index}-{doc_id}-{self._sanitize(filename)}.upload"
        target = os.path.join(self.drop_dir, key)
        async with aiofiles.open(target, "wb") as f:
            async for chunk in gen:
                await f.write(chunk)
                self.current_size += len(chunk)

        # get size of metadata
        metadata = FileMetadata(filename, target_elastic, index, doc_id, target)

        # writing this file makes it available for the uploader
        await metadata.write()

    async def get_files(self):
        # aiofiles.os.listdir is not really async...
        for file in await listdir(self.drop_dir):
            if not file.endswith(".metadata"):
                continue
            yield await FileMetadata.read(os.path.join(self.drop_dir, file))
