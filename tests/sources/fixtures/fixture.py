#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
# ruff: noqa: T203
import asyncio
import functools
import importlib
import importlib.util
import logging
import os
import pprint
import signal
import sys
import time
from argparse import ArgumentParser

import requests
from elastic_transport import ConnectionTimeout
from elasticsearch import ApiError
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth

from connectors.es.client import ESClient
from connectors.logger import set_extra_logger
from connectors.utils import (
    RetryStrategy,
    retryable,
    time_to_sleep_between_retries,
)

CONNECTORS_INDEX = ".elastic-connectors"

logger = logging.getLogger("ftest")
set_extra_logger(logger, log_level=logging.DEBUG, prefix="FTEST")


@retryable(retries=3, interval=1)
def wait_for_es(url="http://localhost:9200", user="elastic", password="changeme"):
    logger.info("Waiting for Elasticsearch to be up and running")

    basic = HTTPBasicAuth(user, password)
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=[500, 502, 503, 504])
    session.mount("http://", HTTPAdapter(max_retries=retries))

    try:
        return session.get(url, auth=basic).json()
    except Exception as e:
        # we're going to display some docker info here
        logger.error(f"Failed to reach out to Elasticsearch {repr(e)}")
        os.system("docker ps -a")
        os.system("docker logs es01")
        raise


def _parser():
    parser = ArgumentParser(prog="fixture")

    parser.add_argument(
        "--action",
        type=str,
        default="load",
        choices=[
            "load",
            "remove",
            "start_stack",
            "check_stack",
            "stop_stack",
            "setup",
            "teardown",
            "monitor",
            "get_num_docs",
            "description",
        ],
    )

    parser.add_argument("--name", type=str, help="fixture to run", default="mysql")

    parser.add_argument("--pid", type=int, help="process id to kill", default=0)

    return parser


def retrying_transient_errors(retries=5):
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args, **kwargs):
            retry = 1
            while retry <= retries:
                try:
                    return await func(*args, **kwargs)
                except ConnectionTimeout:
                    if retry >= retries:
                        raise
                    retry += 1
                except ApiError as e:
                    if e.status_code != 429:
                        raise
                    if retry >= retries:
                        raise
                    retry += 1
                finally:
                    # TODO: 15 is arbitrary
                    # Default retry time is 15 * SUM(1, 5) = 225 seconds, should be enough as a start
                    time_to_sleep = time_to_sleep_between_retries(
                        RetryStrategy.LINEAR_BACKOFF, 15, retry
                    )
                    await asyncio.sleep(time_to_sleep)

        return wrapped

    return wrapper


def _es_client():
    options = {
        "host": "http://127.0.0.1:9200",
        "username": "elastic",
        "password": "changeme",
    }
    return ESClient(options)


@retrying_transient_errors()
async def _wait_for_connectors_index(es_client):
    start = time.time()
    index_present_timeout = 30  # 30 seconds
    waiting_for_connectors_index = True
    while waiting_for_connectors_index:
        if await es_client.index_exists(CONNECTORS_INDEX):
            logger.info(f"{CONNECTORS_INDEX} detected")
            waiting_for_connectors_index = False
            break
        else:
            if time.time() - start > index_present_timeout:
                msg = f"{CONNECTORS_INDEX} not present after {index_present_timeout} seconds."
                raise Exception(msg)

            logger.info(f"{CONNECTORS_INDEX} not present, waiting...")
            await asyncio.sleep(1)


@retrying_transient_errors()
async def _fetch_connector_metadata(es_client):
    # There's only one connector, so we just fetch it always
    response = await es_client.client.search(index=CONNECTORS_INDEX, size=1)
    total_connectors = response["hits"]["total"]["value"]
    if total_connectors == 0:
        msg = "No connectors found in the deployment"
        raise Exception(msg)
    elif total_connectors > 1:
        msg = f"Found {total_connectors} connectors in the deployment, expected only 1"
        raise Exception(msg)

    connector = response["hits"]["hits"][0]

    connector_id = connector["_id"]
    last_synced = connector["_source"]["last_synced"]

    return (connector_id, last_synced)


async def _monitor_service(pid):
    es_client = _es_client()
    sync_job_timeout = 20 * 60  # 20 minutes timeout

    try:
        await _wait_for_connectors_index(es_client)

        start = time.time()

        # Fetch connector
        connector_id, last_synced = await _fetch_connector_metadata(es_client)
        while True:
            _, new_last_synced = await _fetch_connector_metadata(es_client)
            lapsed = time.time() - start
            if last_synced != new_last_synced or lapsed > sync_job_timeout:
                if lapsed > sync_job_timeout:
                    logger.error(
                        f"Took too long to complete the sync job (over {sync_job_timeout} minutes), give up!"
                    )
                break
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"Failed to monitor the sync job. Something bad happened: {e}")
        raise
    finally:
        # the process should always be killed, no matter the monitor succeeds, times out or raises errors.
        logger.debug(f"Trying to kill the process with PID '{pid}'")
        os.kill(pid, signal.SIGINT)
        await es_client.close()


async def _exec_shell(cmd):
    # Create subprocess
    proc = await asyncio.create_subprocess_shell(cmd)

    logger.debug(f"Executing cmd: {cmd}")
    await proc.wait()
    logger.debug(f"Successfully executed cmd: {cmd}")


async def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    action = args.action

    logger.info(f"Executing action: '{action}'.")

    if action in ("start_stack", "stop_stack"):
        os.chdir(os.path.join(os.path.dirname(__file__), args.name))
        if action == "start_stack":
            await _exec_shell("docker compose pull")
            await _exec_shell("docker compose up -d")
        else:
            await _exec_shell("docker compose down --volumes")
        return

    if action == "monitor":
        if args.pid == 0:
            logger.error(f"Invalid pid {args.pid} specified, exit the monitor process.")
            return
        await _monitor_service(args.pid)
        return

    fixture_file = os.path.join(os.path.dirname(__file__), args.name, "fixture.py")
    module_name = f"fixtures.{args.name}.fixture"
    spec = importlib.util.spec_from_file_location(module_name, fixture_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    if hasattr(module, args.action):
        func = getattr(module, args.action)
        if args.action in ("setup", "load", "teardown", "remove"):
            return await func()
        return func()
    else:
        if action == "get_num_docs":
            # returns default
            match os.environ.get("DATA_SIZE", "medium"):
                case "small":
                    print("750")
                case "medium":
                    print("1500")
                case _:
                    print("3000")
        elif action == "description":
            logger.info(
                f'Running an e2e test for {args.name} with a {os.environ.get("DATA_SIZE", "medium")} corpus.'
            )
        elif action == "check_stack":
            # default behavior: we wait until elasticsearch is responding
            pprint.pprint(wait_for_es())
        else:
            logger.warning(
                f"Fixture {args.name} does not have an {args.action} action, skipping."
            )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
