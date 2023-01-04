#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import pytest

from connectors.es.index import ESIndex


@pytest.mark.asyncio
async def test_es_index_create_object_error(mock_responses, patch_logger):
    headers = {"X-Elastic-Product": "Elasticsearch"}
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    index = ESIndex("index", config)
    mock_responses.post(
        "http://nowhere.com:9200/index/_refresh", headers=headers, status=200
    )

    mock_responses.post(
        "http://nowhere.com:9200/index/_search?expand_wildcards=hidden",
        headers=headers,
        status=200,
        payload={"hits": {"total": {"value": 1}, "hits": [{"id": 1}]}},
    )
    with pytest.raises(NotImplementedError) as _:
        async for doc_ in index.get_all_docs():
            pass
