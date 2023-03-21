#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools
import os
import shutil
from collections import defaultdict
from tempfile import mkdtemp

import pytest

from connectors.services.fstreamer.uploader import FileUploadService
from connectors.tests.commons import create_service

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.yml")


@pytest.mark.asyncio
async def test_run_upload_service(mock_responses):
    root = mkdtemp()

    headers = {"X-Elastic-Product": "Elasticsearch"}
    # mock_responses.put(
    #        "http://nowhere:9200/search-dir/_doc/e38117cc6d8183deeed85a9a9d7d0341?pipeline=attachment",
    #        payload={"result": "created"},
    #        headers=headers
    # )

    service = create_service(
        "streamer",
        CONFIG_FILE,
        {"root_attachments": root, "elasticsearch.password": "changeme"},
    )

    asyncio.get_running_loop().call_later(0.5, service.shutdown)
    await service.run()
    shutil.rmtree(root)
