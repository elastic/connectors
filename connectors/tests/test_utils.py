import pytest
import base64
from connectors.utils import (
    next_run,
    validate_index_name,
    InvalidIndexNameError,
    ESClient,
)


def test_next_run():

    assert next_run("1 * * * * *") < 70.0
    assert next_run("* * * * * *") == 0

    # this should get parsed
    next_run("0/5 14,18,52 * ? JAN,MAR,SEP MON-FRI 2002-2010")


def test_invalid_names():
    for name in (
        "index?name",
        "index#name",
        "_indexname",
        "-indexname",
        "+indexname",
        "INDEXNAME",
        "..",
        ".",
    ):
        with pytest.raises(InvalidIndexNameError):
            validate_index_name(name)


def test_esclient():
    # creating a client with a minimal config should create one with sane
    # defaults
    config = {"username": "elastic", "password": "changeme"}
    es_client = ESClient(config)
    assert es_client.host == "http://localhost:9200"

    # XXX find a more elegant way
    assert es_client.client._retry_on_timeout
    basic = f"Basic {base64.b64encode(b'elastic:changeme').decode()}"
    assert es_client.client._headers["Authorization"] == basic
