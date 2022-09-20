#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
from unittest import mock
from connectors.kibana import main


HERE = os.path.dirname(__file__)


def mock_index_creation(index, mock_responses, hidden=True):
    url = f"http://nowhere.com:9200/{index}"
    if hidden:
        url += "?expand_wildcards=hidden"
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

    mock_index_creation(".elastic-connectors", mock_responses)
    mock_responses.put(
        "http://nowhere.com:9200/.elastic-connectors/_doc/1",
        headers=headers,
    )
    mock_index_creation(".elastic-connectors-sync-jobs", mock_responses)
    mock_index_creation("data", mock_responses, hidden=False)

    assert (
        main(
            [
                "--config-file",
                os.path.join(HERE, "config.yml"),
                "--service-type",
                "fake",
                "--index-name",
                "data",
            ]
        )
        == 0
    )
    patch_logger.assert_present("Done")
