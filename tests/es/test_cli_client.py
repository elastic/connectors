from connectors.es.cli_client import CLIClient
from connectors.es.client import X_ELASTIC_PRODUCT_ORIGIN_HEADER


def test_overrides_product_origin_header():
    config = {
        "username": "elastic",
        "password": "changeme",
        "host": "http://nowhere.com:9200",
    }
    cli_client = CLIClient(config)

    assert (
        cli_client.client._headers[X_ELASTIC_PRODUCT_ORIGIN_HEADER] == "connectors-cli"
    )
