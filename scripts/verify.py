#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import os
import asyncio
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
import yaml
from elasticsearch import AsyncElasticsearch

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")


async def verify(service_type, index_name, size, config):
    config = config["elasticsearch"]
    host = config["host"]
    auth = config["username"], config["password"]
    client = AsyncElasticsearch(hosts=[host], basic_auth=auth, request_timeout=120)

    try:
        print(f"Verifying {index_name}...")
        resp = await client.count(index=index_name)
        count = resp["count"]

        print(f"Found {count} documents")
        if count < size:
            raise Exception(f"We want {size} docs")
        print("🤗")
    finally:
        await client.close()


def _parser():
    parser = ArgumentParser(
        prog="verify", formatter_class=ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--config-file", type=str, help="Configuration file", default=DEFAULT_CONFIG
    )
    parser.add_argument(
        "--service-type", type=str, help="Service type", default="mongo"
    )
    parser.add_argument(
        "--index-name", type=str, help="Elasticsearch index", default="search-mongo"
    )
    parser.add_argument("--size", type=int, help="How many docs", default=10001)
    return parser


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    config_file = args.config_file

    if not os.path.exists(config_file):
        raise IOError(f"{config_file} does not exist")

    with open(config_file) as f:
        config = yaml.safe_load(f)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            verify(args.service_type, args.index_name, args.size, config)
        )
        print("Bye")
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("Bye")


if __name__ == "__main__":
    main()
