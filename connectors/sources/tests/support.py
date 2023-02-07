#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os

import yaml
from elasticsearch import AsyncElasticsearch

from connectors.source import DataSourceConfiguration

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "fixtures", "config.yml")


class ESIndexAssertionHelper:
    TIMEOUT_SEC = 120

    def __init__(self, index_name, config=None):
        if config is None:
            # TODO: pass config file directly as dict and load it elsewhere
            with open(DEFAULT_CONFIG) as f:
                config = yaml.safe_load(f)

        print(config)
        config = config["elasticsearch"]
        host = config["host"]
        username = config["username"]
        password = config["password"]

        self.index_name = index_name
        self.client = AsyncElasticsearch(
            hosts=[host],
            basic_auth=(username, password),
            request_timeout=ESIndexAssertionHelper.TIMEOUT_SEC,
        )

    async def assert_has_doc_count(self, expected_count):
        await self.client.indices.refresh(index=self.index_name)

        count_response = await self.client.count(index=self.index_name)
        doc_count = count_response["count"]

        return (
            doc_count == expected_count,
            f"Expected {expected_count} docs. {doc_count} docs are present.",
        )

    async def assert_pipeline_ran(self):
        first_doc = await self._first_doc()

        return "_extract_binary_content" in first_doc, "The pipeline did not run"

    async def assert_first_doc_is_correct(self):
        first_doc = await self._first_doc()

        return len(first_doc.keys()) < 4, "The doc does not look right"

    async def assert_content_extraction_successful(self):
        first_doc = await self._first_doc()

        return "_attachment" in first_doc, "Content extraction was not successful"

    async def close(self):
        await self.client.close()

    async def _first_doc(self):
        docs = await self.client.search(index=self.index_name, query={"match_all": {}})
        print(f"DOCS: {str(docs)}")
        return docs["hits"]["hits"][0]["_source"]


def create_source(klass, **extras):
    config = klass.get_default_configuration()
    for k, v in extras.items():
        config[k] = {"value": v}

    return klass(configuration=DataSourceConfiguration(config))


async def assert_basics(klass, field, value):
    config = DataSourceConfiguration(klass.get_default_configuration())
    assert config[field] == value
    source = create_source(klass)
    await source.ping()
    await source.changed()
