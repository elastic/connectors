#!/usr/bin/python3
import os

_AGENTS = """\
agents:
  provider: "gcp"
  machineType: "n1-standard-8"
  useVault: true
  image: family/enterprise-search-ubuntu-2204-connectors-py
"""

with open(os.path.join(os.path.dirname(__file__), "nightly_steps.yml")) as f:
    steps = f.read().strip()


print(_AGENTS)
print()
print(steps)
