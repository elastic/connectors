#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os

from hatchling.metadata.plugin.interface import MetadataHookInterface


class JSONMetaDataHook(MetadataHookInterface):
    def update(self, metadata):
        src_file = os.path.join(self.root, "connectors", "VERSION")
        with open(src_file) as src:
            version = src.read().strip()
            metadata["version"] = version
