#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Command Line Interface.

Parses arguments and call run() with them.
"""
import asyncio
import functools
import logging
import os
import signal
from argparse import ArgumentParser

from connectors import __version__
from connectors.config import load_config
from connectors.logger import logger, set_logger
from connectors.preflight_check import PreflightCheck
from connectors.services.job_cleanup import JobCleanUpService
from connectors.services.sync import SyncService
from connectors.source import get_data_sources
from connectors.utils import get_event_loop


def _parser():
    parser = ArgumentParser(prog="elastic-ingest")

    parser.add_argument(
        "--action",
        type=str,
        default="poll",
        choices=["poll", "list"],
        help="What elastic-ingest should do",
    )

    parser.add_argument(
        "-c",
        "--config-file",
        type=str,
        help="Configuration file",
        default=os.path.join(os.path.dirname(__file__), "..", "config.yml"),
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Run the event loop in debug mode.",
    )

    parser.add_argument(
        "--filebeat",
        action="store_true",
        default=False,
        help="Output in filebeat format.",
    )

    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Display the version and exit.",
    )

    parser.add_argument(
        "--uvloop",
        action="store_true",
        default=False,
        help="Use uvloop if possible",
    )

    return parser


async def _start_service(config, loop):
    preflight = PreflightCheck(config)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(preflight.shutdown, sig))
    try:
        if not await preflight.run():
            return -1
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)

    services = [SyncService(config), JobCleanUpService(config)]
    for sig in (signal.SIGINT, signal.SIGTERM):

        async def _shutdown(sig_name):
            logger.info(f"Caught {sig_name}. Graceful shutdown.")
            for service in services:
                logger.info(f"Shutdown {service.__class__.__name__}...")
                await service.stop()
            return

        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(_shutdown(sig.name)))

    async def _run_service(service):
        if "PERF8" in os.environ:
            import perf8

            async with perf8.measure():
                return await service.run()
        else:
            return await service.run()

    await asyncio.gather(*[_run_service(service) for service in services])


def run(args):
    """Runner"""

    # load config
    config = load_config(args.config_file)

    # just display the list of connectors
    if args.action == "list":
        logger.info("Registered connectors:")
        for source in get_data_sources(config):
            logger.info(f"- {source.__doc__.strip()}")
        logger.info("Bye")
        return 0

    loop = get_event_loop(args.uvloop)
    coro = _start_service(config, loop)

    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return 0
    finally:
        logger.info("Bye")

    return -1


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    if args.version:
        print(__version__)
        return 0
    set_logger(args.debug and logging.DEBUG or logging.INFO, filebeat=args.filebeat)
    return run(args)
