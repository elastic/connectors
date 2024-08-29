import asyncio
import functools
import signal

from elastic_agent_client.util.async_tools import (
    sleeps_for_retryable,
)
from elastic_agent_client.util.logger import logger

from connectors.agent.component import ConnectorsAgentComponent


def main(args=None):
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
