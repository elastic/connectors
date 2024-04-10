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

import click
from click import ClickException, UsageError

from connectors import __version__
from connectors.config import load_config
from connectors.content_extraction import ContentExtraction
from connectors.logger import logger, set_logger
from connectors.preflight_check import PreflightCheck
from connectors.services import get_services
from connectors.source import get_source_klass, get_source_klasses

__all__ = ["main"]

from connectors.utils import sleeps_for_retryable


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

    multi_service = get_services(actions, config)

    def _shutdown(signal_name):
        sleeps_for_retryable.cancel(signal_name)
        multi_service.shutdown(signal_name)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(_shutdown, sig.name))

    if "PERF8" in os.environ:
        import perf8

        async with perf8.measure():
            return await multi_service.run()
    else:
        return await multi_service.run()


def get_event_loop(uvloop=False):
    if uvloop:
        # activate uvloop if lib is present
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except Exception:
            logger.warning(
                "Unable to enable uvloop: {e}. Running with default event loop"
            )
            pass
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.get_event_loop_policy().get_event_loop()
        if loop is None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
    return loop


def run(action, config_file, log_level, filebeat, service_type, uvloop):
    """Loads the config file, sets the logger and executes an action.

    Actions:
    - list: prints out a list of all connectors and exits
    - poll: starts the event loop and run forever (default)
    """
    logger.info(f"Running connector service version {__version__}")

    # load config
    config = {}
    try:
        config = load_config(config_file)
        ContentExtraction.set_extraction_config(
            config.get("extraction_service", None)
        )  # Not perfect, let's revisit
    except Exception as e:
        # If something goes wrong while parsing config file, we still want
        # to set up the logger so that Cloud deployments report errors to
        # logs properly
        set_logger(logging.INFO, filebeat=filebeat)
        msg = f"Could not parse {config_file}. Check logs for more information"
        logger.exception(f"{msg}.\n{e}")
        raise ClickException(msg) from e

    # Precedence: CLI args >> Config Setting >> INFO
    set_logger(
        log_level or config["service"]["log_level"] or logging.INFO,
        filebeat=filebeat,
    )

    # just display the list of connectors
    if action == ("list",):
        print("Registered connectors:")  # noqa: T201
        for source in get_source_klasses(config):
            print(f"- {source.name}")  # noqa: T201
        print("Bye")  # noqa: T201
        return 0

    if action == ("config",):
        print(  # noqa: T201
            f"Getting default configuration for service type {service_type}"
        )

        source_list = config["sources"]
        if service_type not in source_list:
            msg = f"Could not find a connector for service type {service_type}"
            raise UsageError(msg)

        source_klass = get_source_klass(source_list[service_type])
        print(  # noqa: T201
            json.dumps(source_klass.get_simple_configuration(), indent=2)
        )
        print("Bye")  # noqa: T201
        return 0

    if "list" in action:
        msg = "Cannot use the `list` action with other actions"
        raise UsageError(msg)

    if "config" in action:
        msg = "Cannot use the `config` action with other actions"
        raise UsageError(msg)

    loop = get_event_loop(uvloop)
    coro = _start_service(action, config, loop)

    try:
        return loop.run_until_complete(coro)
    except asyncio.CancelledError:
        return 0
    finally:
        logger.info("Bye")


@click.command()
@click.version_option(__version__, "-v", "--version", message="%(version)s")
@click.option(
    "--action",
    type=click.Choice(
        [
            "schedule",
            "sync_content",
            "sync_access_control",
            "list",
            "config",
            "cleanup",
        ],
        case_sensitive=False,
    ),
    multiple=True,
    default=["schedule", "sync_content", "sync_access_control", "cleanup"],
    help="What elastic-ingest should do.",
)
@click.option(
    "-c",
    "--config-file",
    type=click.Path(),
    default=os.path.join(os.path.dirname(__file__), "..", "config.yml"),
    show_default=True,
    help="Configuration file.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Set log level for the service.",
)
@click.option(
    "--debug",
    "log_level",
    flag_value="DEBUG",
    help="Run the event loop in debug mode (alias for --log-level DEBUG).",
)
@click.option(
    "--filebeat", is_flag=True, default=False, help="Output in filebeat format."
)
@click.option(
    "--service-type",
    type=str,
    default=None,
    help="Service type to get default configuration for if action is config.",
)
@click.option("--uvloop", is_flag=True, default=False, help="Use uvloop if possible.")
def main(action, config_file, log_level, filebeat, service_type, uvloop):
    """Entry point to the service, responsible for all operations.

    Parses the arguments and calls `run` with them.
    """

    return run(action, config_file, log_level, filebeat, service_type, uvloop)
