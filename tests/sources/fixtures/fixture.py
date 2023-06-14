#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import importlib
import importlib.util
import os
import pprint
import signal
import sys
import time
from argparse import ArgumentParser

import requests
from elasticsearch import Elasticsearch
from requests.adapters import HTTPAdapter, Retry
from requests.auth import HTTPBasicAuth
from retry import retry

CONNECTORS_INDEX = ".elastic-connectors"


@retry(tries=3, delay=1.0)
def wait_for_es(url="http://localhost:9200", user="elastic", password="changeme"):
    print("Waiting for Elasticsearch to be up and running")
    basic = HTTPBasicAuth(user, password)
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.0, status_forcelist=[500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    try:
        return s.get(url, auth=basic).json()
    except Exception as e:
        # we're going to display some docker info here
        print(f"Failed to reach out Elasticsearch {repr(e)}")
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


def _es_client():
    options = {
        "hosts": ["http://127.0.0.1:9200"],
        "basic_auth": ("elastic", "changeme"),
    }
    return Elasticsearch(**options)


def _monitor_service(pid):
    es_client = _es_client()
    timeout = 20 * 60  # 20 minutes timeout
    try:
        # we should have something like connectorIndex.search()[0].last_synced
        # once we have ConnectorIndex and Connector class ready
        start = time.time()
        while True:
            response = es_client.search(index=CONNECTORS_INDEX, size=1)

            if len(response["hits"]["hits"]) == 0:
                if time.time() - start > 30:
                    raise Exception(f"{CONNECTORS_INDEX} not present after 30s")

                print(f"{CONNECTORS_INDEX} not present, waiting...")
                time.sleep(1)
            else:
                print(f"{CONNECTORS_INDEX} detected")
                break

        start = time.time()
        connector = response["hits"]["hits"][0]
        connector_id = connector["_id"]
        last_synced = connector["_source"]["last_synced"]
        while True:
            response = es_client.get(index=CONNECTORS_INDEX, id=connector_id)
            new_last_synced = response["_source"]["last_synced"]
            lapsed = time.time() - start
            if last_synced != new_last_synced or lapsed > timeout:
                if lapsed > timeout:
                    print("Took too long to complete the sync job, give up!")
                break
            time.sleep(1)
    except Exception as e:
        print(f"Failed to monitor the sync job. Something bad happened: {e}")
    finally:
        # the process should always be killed, no matter the monitor succeeds, times out or raises errors.
        os.kill(pid, signal.SIGINT)
        es_client.close()


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)

    if args.action in ("start_stack", "stop_stack"):
        os.chdir(os.path.join(os.path.dirname(__file__), args.name))
        if args.action == "start_stack":
            os.system("docker compose pull")
            os.system("docker compose up -d")
        else:
            os.system("docker compose down --volumes")
        return

    if args.action == "monitor":
        if args.pid == 0:
            print("Invalid pid specified, exit the monitor process.")
            return
        _monitor_service(args.pid)
        return

    fixture_file = os.path.join(os.path.dirname(__file__), args.name, "fixture.py")
    module_name = f"fixtures.{args.name}.fixture"
    spec = importlib.util.spec_from_file_location(module_name, fixture_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    if hasattr(module, args.action):
        func = getattr(module, args.action)
        return func()
    else:
        if args.action == "get_num_docs":
            # returns default
            match os.environ.get("DATA_SIZE", "medium"):
                case "small":
                    print("750")
                case "medium":
                    print("1500")
                case _:
                    print("3000")
        elif args.action == "description":
            print(
                f'Running an e2e test for {args.name} with a {os.environ.get("DATA_SIZE", "medium")} corpus.'
            )
        elif args.action == "check_stack":
            # default behavior: we wait until elasticsearch is responding
            pprint.pprint(wait_for_es())
        else:
            print(
                f"Fixture {args.name} does not have an {args.action} action, skipping"
            )


if __name__ == "__main__":
    main()
