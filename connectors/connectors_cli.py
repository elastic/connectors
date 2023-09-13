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
# import asyncio
# import functools
# import json
# import logging
import os
# import signal
# from argparse import ArgumentParser

# from connectors.config import load_config
# from connectors.logger import logger, set_logger
# from connectors.preflight_check import PreflightCheck
# from connectors.services import get_services
# from connectors.source import get_source_klass, get_source_klasses
# from connectors.utils import ExtractionService, get_event_loop

# from connectors.es.settings import DEFAULT_LANGUAGE, Mappings, Settings
# from connectors.es.sink import SyncOrchestrator
# from connectors.logger import logger, set_logger
# from connectors.source import get_source_klass
# from connectors.utils import validate_index_name

import click
import yaml
from connectors.cli.auth import Auth
from connectors import __version__  # NOQA


__all__ = ["main"]

CONNECTORS_INDEX = ".elastic-connectors"
JOBS_INDEX = ".elastic-connectors-sync-jobs"
DEFAULT_CONFIG = os.path.join(os.path.dirname(__file__), "..", "config.yml")
DEFAULT_FILTERING = [
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
]
DEFAULT_PIPELINE = {
    "version": 1,
    "description": "For testing",
    "processors": [
        {
            "remove": {
                "tag": "remove_meta_fields",
                "description": "Remove meta fields",
                "field": [
                    "_attachment",
                    "_attachment_indexed_chars",
                    "_extracted_attachment",
                    "_extract_binary_content",
                    "_reduce_whitespace",
                    "_run_ml_inference",
                ],
                "ignore_missing": True,
            }
        }
    ],
}

# This should be updated when ftest starts to hate on it for any reason
# Later we won't need it at all
JOB_INDEX_MAPPINGS = {
    "dynamic": "false",
    "_meta": {"version": 1},
    "properties": {
        "cancelation_requested_at": {"type": "date"},
        "canceled_at": {"type": "date"},
        "completed_at": {"type": "date"},
        "connector": {
            "properties": {
                "configuration": {"type": "object"},
                "filtering": {
                    "properties": {
                        "advanced_snippet": {
                            "properties": {
                                "created_at": {"type": "date"},
                                "updated_at": {"type": "date"},
                                "value": {"type": "object"},
                            }
                        },
                        "domain": {"type": "keyword"},
                        "rules": {
                            "properties": {
                                "created_at": {"type": "date"},
                                "field": {"type": "keyword"},
                                "id": {"type": "keyword"},
                                "order": {"type": "short"},
                                "policy": {"type": "keyword"},
                                "rule": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                                "value": {"type": "keyword"},
                            }
                        },
                        "warnings": {
                            "properties": {
                                "ids": {"type": "keyword"},
                                "messages": {"type": "text"},
                            }
                        },
                    }
                },
                "id": {"type": "keyword"},
                "index_name": {"type": "keyword"},
                "language": {"type": "keyword"},
                "pipeline": {
                    "properties": {
                        "extract_binary_content": {"type": "boolean"},
                        "name": {"type": "keyword"},
                        "reduce_whitespace": {"type": "boolean"},
                        "run_ml_inference": {"type": "boolean"},
                    }
                },
                "service_type": {"type": "keyword"},
            }
        },
        "created_at": {"type": "date"},
        "deleted_document_count": {"type": "integer"},
        "error": {"type": "keyword"},
        "indexed_document_count": {"type": "integer"},
        "indexed_document_volume": {"type": "integer"},
        "last_seen": {"type": "date"},
        "metadata": {"type": "object"},
        "started_at": {"type": "date"},
        "status": {"type": "keyword"},
        "total_document_count": {"type": "integer"},
        "trigger_method": {"type": "keyword"},
        "worker_hostname": {"type": "keyword"},
    },
}

# Main group
def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(__version__)

# @TODO print help page when no arguments passed
@click.group(invoke_without_command=True)
@click.option('-v', '--version', is_flag=True, callback=print_version,
              expose_value=False, is_eager=True)
@click.option('-c', '--config', type=click.File('rb'))
@click.pass_context
def cli(ctx, config):
    if config:
        ctx.ensure_object(dict)
        ctx.obj['config'] = yaml.safe_load(config)
    pass

    # ctx = click.get_current_context()
    # click.echo(ctx.get_help())
    # ctx.exit()

@click.command(help="Authenticate Connectors CLI with an Elasticsearch instance")
@click.option('--host', prompt="Elastic host")
@click.option('--username', prompt="Username")
@click.option('--password', prompt="Password", hide_input=True)

def login(host, username, password):
    auth = Auth(host, username, password)
    if auth.is_config_present():
        click.confirm(click.style('Config is already present. Are you sure you want to override it?😱', fg='yellow'), abort=True)
    if auth.authenticate():
        click.echo(click.style("Authentication successful. You're breathtaking.", fg='green'))
    else:
        click.echo('')
        click.echo(click.style("Authentication failed. Please check your credentials.", fg='red'), err=True)
    return
   


cli.add_command(login)

# Connector group
@click.group(invoke_without_command=True)
@click.pass_obj
def connector(obj):
    click.echo('testing connector')

cli.add_command(connector)


# Index group
@click.group(invoke_without_command=True)
@click.pass_obj
def index(obj):
    click.echo('testing index')

cli.add_command(index)

# Job group
@click.group(invoke_without_command=True)
@click.pass_obj
def job(obj):
    click.echo('testing job')

cli.add_command(job)


# def run(args):
#     """Loads the config file, sets the logger and executes an action.
#     Actions:
#     - list: prints out a list of all connectors and exits
#     - poll: starts the event loop and run forever (default)
#     """

#     print(f"Framework version is {__version__}")

#     # # load config
#     config = {}
#     try:
#         config = load_config(args.config_file)
#         import pdb; pdb.set_trace();
#     except Exception as e:
#     #     # If something goes wrong while parsing config file, we still want
#     #     # to set up the logger so that Cloud deployments report errors to
#     #     # logs properly
#         print(f"Could not parse {args.config_file}:\n{e}")
#         raise

#     # import pdb; pdb.set_trace();
#     # # just display the list of connectors
#     # if args.action == ["list"]:
#     #     print("Registered connectors:")
#     #     for source in get_source_klasses(config):
#     #         print(f"- {source.name}")
#     #     print("Bye")
#     #     return 0

#     # if args.action == ["config"]:
#     #     service_type = args.service_type
#     #     print(f"Getting default configuration for service type {service_type}")

#     #     source_list = config["sources"]
#     #     if service_type not in source_list:
#     #         print(f"Could not find a connector for service type {service_type}")
#     #         return -1

#     #     source_klass = get_source_klass(source_list[service_type])
#     #     print(json.dumps(source_klass.get_simple_configuration(), indent=2))
#     #     print("Bye")
#     #     return 0

#     # if "list" in args.action:
#     #     print("Cannot use the `list` action with other actions")
#     #     return -1

#     # if "config" in args.action:
#     #     print("Cannot use the `config` action with other actions")
#     #     return -1

#     # loop = get_event_loop(args.uvloop)
#     # coro = _start_service(args.action, config, loop)

#     # try:
#     #     return loop.run_until_complete(coro)
#     # except asyncio.CancelledError:
#     #     return 0
#     # finally:
#     #     logger.info("Bye")

#     return -1


def main(args=None):
    cli()

if __name__ == '__main__':
    main()
