import asyncio
import importlib
import importlib.util
import json
import os
import sys
import time
from argparse import ArgumentParser

from envyaml import EnvYAML

from connectors.kibana import prepare, sync_now
from connectors.logger import logger

DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "config.yml")
DEFAULT_CONNECTOR_CONFIG = {
    "api_key_id": "",
    "status": "configured",
    "language": "en",
    "last_sync_status": "null",
    "last_sync_error": "",
    "last_synced": "",
    "last_seen": "",
    "created_at": "",
    "updated_at": "",
    "filtering": [
        {
            "domain": "DEFAULT",
            "draft": {
                "advanced_snippet": {
                    "updated_at": "2023-01-31T16:41:27.341Z",
                    "created_at": "2023-01-31T16:38:49.244Z",
                    "value": {},
                },
                "rules": [
                    {
                        "field": "_",
                        "updated_at": "2023-01-31T16:41:27.341Z",
                        "created_at": "2023-01-31T16:38:49.244Z",
                        "rule": "regex",
                        "id": "DEFAULT",
                        "value": ".*",
                        "order": 1,
                        "policy": "include",
                    }
                ],
                "validation": {"state": "valid", "errors": []},
            },
            "active": {
                "advanced_snippet": {
                    "updated_at": "2023-01-31T16:41:27.341Z",
                    "created_at": "2023-01-31T16:38:49.244Z",
                    "value": {},
                },
                "rules": [
                    {
                        "field": "_",
                        "updated_at": "2023-01-31T16:41:27.341Z",
                        "created_at": "2023-01-31T16:38:49.244Z",
                        "rule": "regex",
                        "id": "DEFAULT",
                        "value": ".*",
                        "order": 1,
                        "policy": "include",
                    }
                ],
                "validation": {"state": "valid", "errors": []},
            },
        }
    ],
    "scheduling": {"enabled": True, "interval": "1 * * * * *"},
    "pipeline": {
        "extract_binary_content": True,
        "name": "ent-search-generic-ingestion",
        "reduce_whitespace": True,
        "run_ml_inference": True,
    },
    "sync_now": True,
    "is_native": True,
}


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


def _load_connector_config(connector_config_file_path):
    if not connector_config_file_path or not os.path.exists(connector_config_file_path):
        connector_config_file = (
            ("at" + connector_config_file_path) if connector_config_file_path else ""
        )

        logger.warn(
            f"connector config file {connector_config_file} does not exist. Fallback to default connector config."
        )

        connector_config = DEFAULT_CONNECTOR_CONFIG
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
        connector_config = json.load(connector_config_file)
        logger.info(
            f"Successfully loaded connector config file from '{connector_config_file_path}'."
        )

    return connector_config


async def _update(test):
    logger.info("Updating data...")
    await test.update()
    logger.info("Finished updating data.")
    await sync_now()
    logger.info("Verify sync after updating data...")
    await test.verify_update()
    logger.info("Verified sync after updating data.")


async def _remove(test):
    logger.info("Removing data...")
    await test.remove()
    logger.info("Finished removing data.")
    await sync_now()
    logger.info("Verify sync after removing data...")
    await test.verify_remove()
    logger.info("Verified sync after removing data.")


async def _load(test):
    logger.info("Loading data...")
    await test.load()
    logger.info("Finished loading data.")
    await sync_now()
    logger.info("Verify sync after initial load...")
    await test.verify_load()
    logger.info("Verified sync after initial load.")


async def _execute_test(args, test_file, connector_config):
    module_name = f"fixtures.{args.name}.fixture"
    spec = importlib.util.spec_from_file_location(module_name, test_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    index_name = f"search-{args.name}"
    service_type = args.name

    # TODO: find by decorator
    test_class = getattr(module, "Test")
    test = test_class(index_name, args.size)

    # TODO: should be a param
    config = EnvYAML(DEFAULT_CONFIG)

    try:
        await prepare(service_type, index_name, config, connector_config)

        await _load(test)
        await _remove(test)
        await _update(test)

        logger.info("Test execution finished.")
    except Exception as e:
        logger.error("Test execution failed", e)
    finally:
        await test.close()


async def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)

    os.chdir(os.path.join(os.path.dirname(__file__), args.name))
    logger.info("Start stack")
    os.system("docker compose up -d")

    time.sleep(30)

    scenarios_dir = os.path.join(os.path.dirname(__file__), args.name, "scenarios")
    if os.path.exists(scenarios_dir) and os.path.isdir(scenarios_dir):
        scenarios = [
            entry.path for entry in os.scandir(scenarios_dir) if entry.is_dir()
        ]

        for scenario in scenarios:
            print(f"Current scenario dir {scenario}")

            test_file = os.path.join(scenario, "test.py")
            connector_config_file = os.path.join(scenario, "connector.json")
            connector_config = _load_connector_config(connector_config_file)

            await _execute_test(args, test_file, connector_config)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
