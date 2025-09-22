#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools
import signal

from elastic_agent_client.util.async_tools import (
    sleeps_for_retryable,
)

from connectors.agent.component import ConnectorsAgentComponent
from connectors.agent.logger import get_logger
from logging import Logger

logger: Logger = get_logger("cli")


def main(args=None) -> None:
    """Script entry point into running Connectors Service on Agent.

    It initialises an event loop, creates a component and runs the component.
    Additionally, signals are handled for graceful termination of the component.
    """
    loop = asyncio.get_event_loop()
    logger.info("Running agent")
    component = ConnectorsAgentComponent()

    def _shutdown(signal_name):
        sleeps_for_retryable.cancel(signal_name)
        component.stop(signal_name)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(_shutdown, sig.name))

    return loop.run_until_complete(component.run())


if __name__ == "__main__":
    try:
        main()
    finally:
        logger.info("Bye")
