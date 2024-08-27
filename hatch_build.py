import os

from hatchling.metadata.plugin.interface import MetadataHookInterface


class JSONMetaDataHook(MetadataHookInterface):
    def update(self, metadata):
        src_file = os.path.join(self.root, "connectors", "VERSION")
        with open(src_file) as src:
            version = src.read().strip()
            metadata["version"] = version
