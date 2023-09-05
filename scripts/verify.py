#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import os
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

from elasticsearch import AsyncElasticsearch

from connectors.config import load_config

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")
SERVERLESS = "SERVERLESS" in os.environ


async def verify(service_type, index_name, size, config):
    config = config["elasticsearch"]
    host = config["host"]
    auth = config["username"], config["password"]
    client = AsyncElasticsearch(hosts=[host], basic_auth=auth, request_timeout=120)

    if not SERVERLESS:
        await client.indices.refresh(index=index_name)

    try:
        print(f"Verifying {index_name}...")
        resp = await client.count(index=index_name)
        count = resp["count"]

        print(f"Found {count} documents")
        if count < size:
            raise Exception(f"We want {size} docs")

        # checking one doc
        res = await client.search(index=index_name, query={"match_all": {}})
        first_doc = res["hits"]["hits"][0]["_source"]
        print("First doc")
        print(first_doc)

        if len(first_doc.keys()) < 3:
            raise Exception("The doc does not look right")

        if "_extract_binary_content" in first_doc:
            raise Exception("The pipeline did not run")

        if "_attachment" in first_doc:
            raise Exception("Content extraction was not successful")

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
        "--service-type", type=str, help="Service type", default="mongodb"
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

    config = load_config(config_file)

    try:
        asyncio.run(verify(args.service_type, args.index_name, args.size, config))
        print("Bye")
    except (asyncio.CancelledError, KeyboardInterrupt):
        print("Bye")


if __name__ == "__main__":
    main()
