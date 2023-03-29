#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio

import elasticsearch

from connectors.byoei import ElasticServer
from connectors.es.settings import DEFAULT_LANGUAGE, Mappings, Settings
from connectors.logger import logger

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_PIPELINE = {
    "version": 1,
    "description": "For testing",
    "processors": [
        {
            "remove": {
                "tag": "remove_meta_fields",
                "description": "Remove meta fields",
                "field": [
                    "_attachment",
                    "_attachment_indexed_chars",
                    "_extracted_attachment",
                    "_extract_binary_content",
                    "_reduce_whitespace",
                    "_run_ml_inference",
                ],
                "ignore_missing": True,
            }
        }
    ],
}


class KibanaFake:
    def __init__(self, config, index_name):
        self.config = config
        self.index_name = index_name
        self.es = ElasticServer(config["elasticsearch"])

    async def setup(self):
        logger.debug("Preparing initial indices...")

        try:
            await asyncio.gather(
                self.create_pipeline(),
                self.create_connectors_index(),
                self.create_sync_jobs_index()
            )

            logger.info("Finished creating initial indices.")
        finally:
            await self.es.close()

    async def connectors_index_state(self):
        return await self.es.client.search(index=CONNECTORS_INDEX, query={"match_all": {}})

    async def sync_now(self, connector_id):
        logger.debug(f"Setting sync_now to true for connector with id '{connector_id}'...")

        connector_doc = await self.es.client.get(index=CONNECTORS_INDEX, id=connector_id)
        # TODO: remove
        logger.info(f"HELLO: {str(connector_doc)}")
        connector_doc["_source"]["sync_now"] = True

        # TODO: raise here?
        await self.es.client.update(index=CONNECTORS_INDEX, id=connector_id, doc=connector_doc["_source"], retry_on_conflict=3)

        logger.info(f"Set sync_now to true for connector with id '{connector_id}'.")

        await asyncio.sleep(10)

    async def create_pipeline(self):
        logger.debug("Creating an ingestion pipeline...")

        try:
            await self.es.client.ingest.get_pipeline(
                id="ent-search-generic-ingestion"
            )
        except elasticsearch.NotFoundError:
            await self.es.client.ingest.put_pipeline(
                id="ent-search-generic-ingestion", body=DEFAULT_PIPELINE
            )

        try:
            await self.es.client.ingest.get_pipeline(
                id="ent-search-generic-ingestion"
            )
        except elasticsearch.NotFoundError:
            pipeline = {
                "description": "My optional pipeline description",
                "processors": [
                    {
                        "set": {
                            "description": "My optional processor description",
                            "field": "my-keyword-field",
                            "value": "foo",
                        }
                    }
                ],
            }

            await self.es.client.ingest.put_pipeline(
                id="ent-search-generic-ingestion", body=pipeline
            )

        logger.info("Created ingestion pipeline.")

    async def create_index_to_ingest_in(self, index_name):
        logger.debug(f"Creating {index_name} index...")

        mappings = Mappings.default_text_fields_mappings(
            is_connectors_index=True,
        )
        settings = Settings(
            language_code=DEFAULT_LANGUAGE, analysis_icu=False
        ).to_hash()

        await self.upsert_index(index_name, mappings=mappings, settings=settings)

        logger.info(f"Created {index_name} index.")

    async def create_connectors_index(self):
        logger.debug(f"Creating {CONNECTORS_INDEX} index...")

        await self.upsert_index(CONNECTORS_INDEX)

        logger.info(f"Created {CONNECTORS_INDEX} index.")

    async def add_connector(self, config):
        if config is None:
            pass
            # TODO: raise something

        index_name = config["index_name"]
        name = config["name"]
        service_type = config["service_type"]

        logger.debug(f"Creating connector with name '{name}' and service type '{service_type}'...")

        await self.create_index_to_ingest_in(index_name)

        response = await self.es.client.index(index=CONNECTORS_INDEX, document=config)
        connector_doc_id = response["_id"]

        logger.info(f"Created connector (id: '{connector_doc_id}') with name '{name}' and service type '{service_type}'.")

        return connector_doc_id

    async def create_sync_jobs_index(self):
        logger.debug(f"Creating {JOBS_INDEX} index...")

        await self.upsert_index(JOBS_INDEX)

        logger.info(f"Created {JOBS_INDEX} index.")

    async def upsert_index(self, index, mappings=None, settings=None):
        """Override the index with new mappings and settings.

        If the index with such name exists, it's deleted and then created again
        with provided mappings and settings. Otherwise index is just created.

        After that, provided docs are inserted into the index.

        This method is supposed to be used only for testing - framework is not
        supposed to create/delete indices at all, Kibana is responsible for
        this logic.
        """

        if index.startswith("."):
            expand_wildcards = "hidden"
        else:
            expand_wildcards = "open"

        exists = await self.es.client.indices.exists(
            index=index, expand_wildcards=expand_wildcards
        )

        if exists:
            logger.debug(f"{index} exists, deleting...")
            logger.debug("Deleting it first")
            await self.es.client.indices.delete(index=index, expand_wildcards=expand_wildcards)

        logger.debug(f"Creating index {index}")
        await self.es.client.indices.create(index=index, mappings=mappings, settings=settings)
