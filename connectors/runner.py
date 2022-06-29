import os
import asyncio

import yaml

from connectors.backends.mongo import MongoConnector
from connectors.elastic import ElasticServer
from connectors.logger import logger


IDLING = 10


def get_connector_instance(definition):
    return MongoConnector(definition)


async def poll(config):
    es = ElasticServer(config["elasticsearch"])
    await es.prepare()

    while True:
        logger.debug("poll")
        async for definition in es.get_connectors_definitions():
            connector = get_connector_instance(definition)
            await connector.ping()

            es_index = definition["es_index"]
            await es.prepare_index(es_index)
            existing_ids = [doc_id async for doc_id in es.get_existing_ids(es_index)]

            async def get_docs():
                async for doc in connector.get_docs():
                    if doc["_id"] in existing_ids:
                        continue
                    doc_id = doc["_id"]
                    doc["id"] = doc_id
                    del doc["_id"]
                    yield {
                        "_op_type": "update",
                        "_index": es_index,
                        "_id": doc_id,
                        "doc": doc,
                        "doc_as_upsert": True,
                    }

            result = await es.async_bulk(get_docs())
            logger.info(result)

        await asyncio.sleep(IDLING)


def run(args):
    if not os.path.exists(args.config_file):
        raise IOError(f"{args.config_file} does not exist")

    with open(args.config_file) as f:
        config = yaml.safe_load(f)

    logger.info(f"Connecting to {config['elasticsearch']['host']}")
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(poll(config))
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Bye")
