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
from connectors.services.base import MultiService
from connectors.services.job_cleanup import JobCleanUpService
from connectors.services.sync import SyncService
from connectors.source import get_source_klasses
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

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=None,
        help="Set log level for the service.",
    )
    group.add_argument(
        "--debug",
        dest="log_level",
        action="store_const",
        const="DEBUG",
        help="Run the event loop in debug mode (alias for --log-level DEBUG)",
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

    multiservice = MultiService(SyncService(config), JobCleanUpService(config))
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(multiservice.shutdown, sig.name))

    if "PERF8" in os.environ:
        import perf8

        async with perf8.measure():
            return await multiservice.run()
    else:
        return await multiservice.run()


def run(args):
    """Runner"""

    # load config
    config = {}
    try:
        config = load_config(args.config_file)
    except Exception as e:
        # If something goes wrong while parsing config file, we still want
        # to set up the logger so that Cloud deployments report errors to
        # logs properly
        set_logger(logging.INFO, filebeat=args.filebeat)
        logger.exception(f"Could not parse {args.config_file}:\n{e}")
        raise

    # Precedence: CLI args >> Config Setting >> INFO
    set_logger(
        args.log_level or config["service"]["log_level"] or logging.INFO,
        filebeat=args.filebeat,
    )

    # just display the list of connectors
    if args.action == "list":
        print("Registered connectors:")
        for source in get_source_klasses(config):
            print(f"- {source.name}")
        print("Bye")
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
    return run(args)
