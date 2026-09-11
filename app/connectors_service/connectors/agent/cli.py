#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import functools
import signal
import sys

from elastic_agent_client.util.async_tools import (
    sleeps_for_retryable,
)

from connectors.agent.component import ConnectorsAgentComponent
from connectors.agent.logger import get_logger
from connectors.fips import FIPSModeError

logger = get_logger("cli")

# Exit code used when the component stops because of an error, so that Agent can
# tell a failure apart from a clean shutdown
FAILURE_EXIT_CODE = 1


def main(args=None):
    """Script entry point into running Connectors Service on Agent.

    It initialises an event loop, creates a component and runs the component.
    Additionally, signals are handled for graceful termination of the component.

    Returns:
        int: FAILURE_EXIT_CODE if the component stopped because of an error,
            None if it shut down cleanly.
    """
    loop = asyncio.get_event_loop()
    logger.info("Running agent")

    try:
        component = ConnectorsAgentComponent()
    except FIPSModeError as e:
        # Raised while reading FIPS mode from the environment
        logger.error(f"Cannot start connectors agent component: {e}")
        return FAILURE_EXIT_CODE

    def _shutdown(signal_name):
        sleeps_for_retryable.cancel(signal_name)
        component.stop(signal_name)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, functools.partial(_shutdown, sig.name))

    try:
        return loop.run_until_complete(component.run())
    except FIPSModeError as e:
        logger.error(f"Connectors agent component stopped: {e}")
        return FAILURE_EXIT_CODE
    except Exception as e:
        logger.exception(f"Connectors agent component stopped with an error: {e}")
        return FAILURE_EXIT_CODE


if __name__ == "__main__":
    try:
        sys.exit(main())
    finally:
        logger.info("Bye")
