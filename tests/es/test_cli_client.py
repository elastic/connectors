from connectors.es.cli_client import CLIClient


def test_overrides_product_origin_header():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    cli_client = CLIClient(config)

    assert cli_client.client._headers["user-agent"] == CLIClient.user_agent
