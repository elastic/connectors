import os
import asyncio
import importlib

import yaml

from connectors.elastic import ElasticServer
from connectors.logger import logger

IDLING = 10


def get_connector_instance(definition, config):
    logger.debug(f"Getting connector instance for {definition}")
    service_type = definition["service_type"]
    module_name, klass_name = config["connectors"][service_type].split(":")
    module = importlib.import_module(module_name)
    klass = getattr(module, klass_name)
    logger.debug(f"Found a matching plugin {klass}")
    return klass(definition)


async def poll(config):
    es = ElasticServer(config["elasticsearch"])

    while True:
        logger.debug("poll")
        async for definition in es.get_connectors_definitions():
            connector = get_connector_instance(definition, config)
            await connector.ping()

            es_index = definition["es_index"]
            await es.prepare_index(es_index)
            existing_ids = [doc_id async for doc_id in es.get_existing_ids(es_index)]
            result = await es.async_bulk(es_index, connector.get_docs())
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
