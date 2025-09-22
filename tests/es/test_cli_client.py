#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors import __version__
from connectors.es.cli_client import CLIClient


def test_overrides_user_agent_header() -> None:
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    cli_client = CLIClient(config)

    assert (
        cli_client.client._headers["user-agent"]
        == f"elastic-connectors-{__version__}/cli"
    )
