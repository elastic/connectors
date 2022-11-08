#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import pytest

from connectors.source import (
    Field,
    DataSourceConfiguration,
    get_source_klass,
    get_data_sources,
    get_data_source,
    BaseDataSource,
)
from connectors.byoc import BYOConnector, Status


CONFIG = {
    "host": {
        "value": "mongodb://127.0.0.1:27021",
        "label": "MongoDB Host",
        "type": "str",
    },
    "database": {
        "value": "sample_airbnb",
        "label": "MongoDB Database",
        "type": "str",
    },
    "collection": {
        "value": "listingsAndReviews",
        "label": "MongoDB Collection",
        "type": "str",
    },
}


def test_field():
    # stupid holder
    f = Field("name")
    assert f.label == "name"


def test_data_source_configuration():

    c = DataSourceConfiguration(CONFIG)
    assert c["database"] == "sample_airbnb"
    assert c.get_field("database").label == "MongoDB Database"
    assert sorted([f.name for f in c.get_fields()]) == sorted(CONFIG.keys())
    c.set_field("new", value="one")
    assert c["new"] == "one"


def test_default():
    c = DataSourceConfiguration(CONFIG)
    assert c.get("database") == "sample_airbnb"
    assert c.get("dd", 1) == 1


class MyConnector:
    id = "1"
    service_type = "yea"

    def __init__(self, *args):
        pass


def test_get_source_klass():
    assert get_source_klass("test_source:MyConnector") is MyConnector


def test_get_data_sources():
    settings = {
        "sources": {"yea": "test_source:MyConnector", "yea2": "test_source:MyConnector"}
    }

    sources = list(get_data_sources(settings))
    assert sources == [MyConnector, MyConnector]


class Banana(BaseDataSource):
    """Banana"""

    @classmethod
    def get_default_configuration(cls):
        return {"one": {"value": None}}


@pytest.mark.asyncio
async def test_get_custom_data_source_no_service_type(mock_responses):
    class Client:
        pass

    class Index:
        client = Client()

        async def save(self, conn):
            pass

    # generic empty doc created by the user through the Kibana UI
    # when it's created that way, the service type is None,
    # so it's up to the connector to set it back to its value
    doc = {
        "status": "created",
        "service_type": None,
        "index_name": "test",
        "configuration": {},
        "scheduling": {"enabled": False},
    }
    connector = BYOConnector(Index(), "1", doc)

    config = {
        "connector_id": "1",
        "service_type": "banana",
        "sources": {"banana": "connectors.tests.test_source:Banana"},
    }

    source = await get_data_source(connector, config)
    assert str(source) == "Datasource `Banana`"
    assert connector.status == Status.NEEDS_CONFIGURATION


@pytest.mark.asyncio
async def test_base_class():
    class Connector:
        configuration = {}

    base = BaseDataSource(Connector())

    # ABCs
    with pytest.raises(NotImplementedError):
        BaseDataSource.get_default_configuration()

    with pytest.raises(NotImplementedError):
        await base.ping()

    await base.close()

    with pytest.raises(NotImplementedError):
        await base.get_docs()
