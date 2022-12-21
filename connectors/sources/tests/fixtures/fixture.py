#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import importlib
import importlib.util
import os
import sys
import time
from argparse import ArgumentParser


def _parser():
    parser = ArgumentParser(prog="elastic-ingest")

    parser.add_argument(
        "--action",
        type=str,
        default="load",
        choices=["load", "remove", "start_stack", "stop_stack", "setup", "teardown"],
    )

    parser.add_argument("--name", type=str, help="fixture to run", default="mysql")

    return parser


def main(args=None):
    parser = _parser()
    args = parser.parse_args(args=args)

    if args.action in ("start_stack", "stop_stack"):
        os.chdir(os.path.join(os.path.dirname(__file__), args.name))
        if args.action == "start_stack":
            os.system("docker compose up -d")
            # XXX do better
            time.sleep(30)
        else:
            os.system("docker compose down --volumes")
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
        print(f"Fixture {args.name} does not have an {args.action} action, skipping")


if __name__ == "__main__":
    main()
