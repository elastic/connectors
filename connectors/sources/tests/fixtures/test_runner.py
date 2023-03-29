#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import asyncio
import importlib
import importlib.util
import json
import os
import sys
import time
from abc import ABC, abstractmethod
from argparse import ArgumentParser

from envyaml import EnvYAML

from connectors.kibana import KibanaFake
from connectors.logger import logger
from connectors.sources.tests.fixtures.test_environment import DockerTestEnvironment

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")


def _parser():
    parser = ArgumentParser(prog="elastic-ingest")

    parser.add_argument(
        "--action",
        type=str,
        default="load",
        choices=[
            "load",
            "remove",
            "verify",
            "start_stack",
            "stop_stack",
            "setup",
            "teardown",
        ],
    )

    parser.add_argument("--name", type=str, help="fixture to run", default="mysql")

    parser.add_argument("--size", type=int, help="expected doc count", default=-1)

    return parser


class NoTestEnvironmentSpecifiedError(Exception):
    pass


class TestRunner(ABC):

    def __init__(self, test_environment):
        if test_environment is None:
            raise NoTestEnvironmentSpecifiedError

        self.test_environment = test_environment

    @abstractmethod
    def run(self):
        raise NotImplementedError


def _load_connector_config(connector_config_file_path):
    if not connector_config_file_path or not os.path.exists(connector_config_file_path):
        raise FileNotFoundError(f"No connector config file found at '{connector_config_file_path}'")
    else:
        connector_config_file = open(
            os.path.join(
                os.path.dirname(__file__),
                "sources",
                "tests",
                "fixtures",
                connector_config_file_path,
            )
        )

        # TODO: use with?
        connector_config = json.load(connector_config_file)
        logger.info(
            f"Successfully loaded connector config file from '{connector_config_file_path}'."
        )

    return connector_config


async def _load_test_cases(args, test_file):
    module_name = f"fixtures.{args.name}.tests"
    spec = importlib.util.spec_from_file_location(module_name, test_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    index_name = f"search-{args.name}"

    test_class = getattr(module, "Test")
    test = test_class(index_name, args.size)

    return [test]


async def _run_test(test, args, connector_configuration):
    config = EnvYAML(DEFAULT_CONFIG)

    index_name = f"search-{args.name}"

    try:
        kibana_fake = KibanaFake(config, index_name)

        await kibana_fake.setup()
        await asyncio.sleep(10)

        logger.info("Loading data...")
        await test.load()
        logger.info("Finished loading data.")

        await kibana_fake.add_connector(connector_configuration)

        logger.info("Sleep for 30 seconds...")
        await asyncio.sleep(30)
        logger.info("Slept for 30 seconds.")

        logger.info("Verify sync after initial load...")
        await test.verify_load()
        logger.info("Verified sync after initial load.")

        # await _remove(test)
        # await _update(test)

        # logger.info("Test execution finished.")
    except Exception as e:
        logger.error("Test execution failed", e)


async def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)

    test_directory = os.path.join(os.path.dirname(__file__), args.name)
    # TODO: load test specific env (should reside in loop)
    test_environment = DockerTestEnvironment(test_directory)
    await test_environment.setup()

    logger.info("Sleeping for 30 seconds...")
    time.sleep(30)
    logger.info("Continue...")

    connector_configuration = _load_connector_config(os.path.join(test_directory, "connector.json"))
    test_cases = await _load_test_cases(args, os.path.join(test_directory, "tests.py"))

    for test in test_cases:
        try:
            await _run_test(test, args, connector_configuration)
        finally:
            await test.teardown()
            await test_environment.teardown()


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
