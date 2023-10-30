#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Command Line Interface.

This is the main entry point of the framework. When the project is installed as
a Python package, an `elastic-ingest` executable is added in the PATH and
executes the `main` function of this module, which starts the service.
"""
import asyncio
import functools
import json
import logging
import os
import signal
from argparse import ArgumentParser

from connectors import __version__
from connectors.config import load_config
from connectors.content_extraction import ContentExtraction
from connectors.logger import logger, set_logger
from connectors.preflight_check import PreflightCheck
from connectors.services import get_services
from connectors.source import get_source_klass, get_source_klasses
from connectors.utils import get_event_loop

__all__ = ["main"]


def _parser():
    """Parses command-line arguments using ArgumentParser and returns it"""
    parser = ArgumentParser(prog="elastic-ingest")

    parser.add_argument(
        "--action",
        type=str,
        default=["schedule", "execute", "cleanup"],
        choices=["schedule", "execute", "list", "config", "cleanup"],
        nargs="+",
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
        "--service-type",
        type=str,
        default=None,
        help="Service type to get default configuration for if action is config",
    )

    parser.add_argument(
        "--uvloop",
        action="store_true",
        default=False,
        help="Use uvloop if possible",
    )

    return parser


async def _start_service(actions, config, loop):
    """Starts the service.

    Steps:
    - performs a preflight check using `PreflightCheck`
    - instantiates a `MultiService` instance and runs its `run` async function
    """
    preflight = PreflightCheck(config)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(preflight.shutdown, sig))
    try:
        if not await preflight.run():
            return -1
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)

    multiservice = get_services(actions, config)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(multiservice.shutdown, sig.name))

    if "PERF8" in os.environ:
        import perf8

        async with perf8.measure():
            return await multiservice.run()
    else:
        return await multiservice.run()


def run(args):
    """Loads the config file, sets the logger and executes an action.

    Actions:
    - list: prints out a list of all connectors and exits
    - poll: starts the event loop and run forever (default)
    """
    logger.info(f"Running connector service version {__version__}")

    # load config
    config = {}
    try:
        config = load_config(args.config_file)
        ContentExtraction.set_extraction_config(
            config.get("extraction_service", None)
        )  # Not perfect, let's revisit
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
    if args.action == ["list"]:
        print("Registered connectors:")  # noqa: T201
        for source in get_source_klasses(config):
            print(f"- {source.name}")  # noqa: T201
        print("Bye")  # noqa: T201
        return 0

    if args.action == ["config"]:
        service_type = args.service_type
        print(  # noqa: T201
            f"Getting default configuration for service type {service_type}"
        )

        source_list = config["sources"]
        if service_type not in source_list:
            print(  # noqa: T201
                f"Could not find a connector for service type {service_type}"
            )
            return -1

        source_klass = get_source_klass(source_list[service_type])
        print(  # noqa: T201
            json.dumps(source_klass.get_simple_configuration(), indent=2)
        )
        print("Bye")  # noqa: T201
        return 0

    if "list" in args.action:
        print("Cannot use the `list` action with other actions")  # noqa: T201
        return -1

    if "config" in args.action:
        print("Cannot use the `config` action with other actions")  # noqa: T201
        return -1

    loop = get_event_loop(args.uvloop)
    coro = _start_service(args.action, config, loop)

    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return 0
    finally:
        logger.info("Bye")

    return -1


def main(args=None):
    """Entry point to the service, responsible for all operations.

    Parses the arguments and calls `run` with them.
    If `--version` is used, displays the version and exits.
    """
    parser = _parser()
    args = parser.parse_args(args=args)
    if args.version:
        print(__version__)  # noqa: T201
        return 0

    return run(args)
