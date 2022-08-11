#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Command Line Interface.

Parses arguments and call connectors.runner.run with them.
"""
from argparse import ArgumentParser
import os
import logging

from connectors import __version__
from connectors.runner import run
from connectors.logger import set_logger


def _parser():
    parser = ArgumentParser(prog="elastic-ingest")

    parser.add_argument(
        "--action",
        type=str,
        default="poll",
        choices=["poll", "list"],
        help="What elastic-ingest should do",
    )

    parser.add_argument(
        "-c",
        "--config-file",
        type=str,
        help="Configuration file",
        default=os.path.join(os.path.dirname(__file__), "..", "config.yml"),
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Run the event loop in debug mode.",
    )

    parser.add_argument(
        "--filebeat",
        action="store_true",
        default=False,
        help="Output in filebeeat format.",
    )

    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Display the version and exit.",
    )

    parser.add_argument(
        "--one-sync",
        action="store_true",
        default=False,
        help="Runs a single sync and exits.",
    )

    return parser


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)
    if args.version:
        print(__version__)
        return 0
    set_logger(args.debug and logging.DEBUG or logging.INFO, filebeat=args.filebeat)
    return run(args)
