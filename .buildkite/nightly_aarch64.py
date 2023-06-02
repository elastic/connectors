#!/usr/bin/python3
import os
import platform

machine = platform.uname().machine

if machine in ("arm64", "aarch64"):
    _AGENTS = """\
agents:
  provider: aws
  instanceType: m6g.xlarge
  imagePrefix: enterprise-search-ubuntu-2204-aarch64-connectors-py
"""
else:
    _AGENTS = """\
agents:
  provider: "gcp"
  machineType: "n1-standard-8"
  useVault: true
  image: family/enterprise-search-ubuntu-2204-connectors-py
"""

#imagePrefix: enterprise-search-ubuntu-2204-aarch64-connectors-py
#imagePrefix: drivah-ubuntu-2204-aarch64
#
with open(os.path.join(os.path.dirname(__file__), "nightly_steps.yml")) as f:
    steps = f.read().strip()


print(_AGENTS)
print()
print(steps)
