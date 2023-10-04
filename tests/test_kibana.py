#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from unittest import mock

import pytest

from connectors.es.sink import SyncOrchestrator
from connectors.kibana import main, upsert_index

HERE = os.path.dirname(__file__)
FIXTURES_DIR = os.path.abspath(os.path.join(HERE, "fixtures"))


def mock_index_creation(index, mock_responses, hidden=True):
    url = f"http://nowhere.com:9200/{index}"
    if hidden:
        url += "?expand_wildcards=hidden"
    else:
        url += "?expand_wildcards=open"
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.head(
        url,
        headers=headers,
    )
    mock_responses.delete(
        url,
        headers=headers,
    )
    mock_responses.put(
        f"http://nowhere.com:9200/{index}",
        headers=headers,
    )


@mock.patch.dict(os.environ, {"elasticsearch.password": "changeme"})
def test_main(patch_logger, mock_responses):
    headers = {"X-Elastic-Product": "Elasticsearch"}

    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors-v1/_doc/1",
        headers=headers,
    )
    mock_index_creation("data", mock_responses, hidden=False)
    mock_responses.get(
        "http://nowhere.com:9200/_ingest/pipeline/ent-search-generic-ingestion",
        headers=headers,
        repeat=True,
    )

    assert (
        main(
            [
                "--config-file",
                os.path.join(FIXTURES_DIR, "config.yml"),
                "--service-type",
                "fake",
                "--index-name",
                "data",
                "--connector-definition",
                os.path.join(FIXTURES_DIR, "connector.json"),
            ]
        )
        == 0
    )


@pytest.mark.asyncio
async def test_upsert_index(mock_responses):
    config = {"host": "http://nowhere.com:9200", "user": "tarek", "password": "blah"}
    headers = {"X-Elastic-Product": "Elasticsearch"}
    mock_responses.post(
        "http://nowhere.com:9200/.elastic-connectors/_refresh", headers=headers
    )
    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=open",
        headers=headers,
        status=404,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index",
        payload={"_id": "1"},
        headers=headers,
    )

    es = SyncOrchestrator(config)

    await upsert_index(es, "search-new-index")

    mock_responses.head(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=open",
        headers=headers,
    )
    mock_responses.delete(
        "http://nowhere.com:9200/search-new-index?expand_wildcards=open",
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_doc/1",
        payload={"_id": "1"},
        headers=headers,
    )
    mock_responses.put(
        "http://nowhere.com:9200/search-new-index/_doc/2",
        payload={"_id": "2"},
        headers=headers,
    )

    await upsert_index(es, "search-new-index", docs=[{"_id": "2"}, {"_id": "3"}])
