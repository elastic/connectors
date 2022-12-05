#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Event loop

- polls for work by calling Elasticsearch on a regular basis
- instanciates connector plugins
- mirrors an Elasticsearch index with a collection of documents
"""
import asyncio
import signal
import functools

from envyaml import EnvYAML
from connectors.logger import logger
from connectors.source import get_data_sources
from connectors.utils import get_event_loop
from connectors.syncer import ConnectorService
from connectors.fstreamer import FileUploadService


def run(args):
    """Runner"""
    loop = get_event_loop()

    # just display the list of connectors
    if args.action == "list":
        logger.info("Registered connectors:")
        config = EnvYAML(args.config_file)
        for source in get_data_sources(config):
            logger.info(f"- {source.__doc__.strip()}")
        logger.info("Bye")
        return 0

    if args.action == "streamer":
        service = FileUploadService(args)
    else:
        service = ConnectorService(args)

    coro = service.run()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(service.shutdown, sig))

    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return 0
    finally:
        logger.info("Bye")

    return -1
