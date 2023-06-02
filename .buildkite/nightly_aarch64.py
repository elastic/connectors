#!/usr/bin/python3
import os

_AGENTS = """\
agents:
  provider: aws
  instanceType: m6g.xlarge
  imagePrefix: enterprise-search-ubuntu-2204-aarch64-connectors-py
"""

with open(os.path.join(os.path.dirname(__file__), "nightly_steps.yml")) as f:
    steps = f.read().strip()


print(_AGENTS)
print()
print(steps)
