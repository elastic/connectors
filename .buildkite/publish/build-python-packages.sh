#!/usr/bin/env bash
# Generates the dependency CSV using pip-licenses in a Python-capable agent image.
# Outputs are uploaded as Buildkite artifacts by the pipeline step.
set -euo pipefail

source .buildkite/shared.sh

PYTHON_VERSION="${DRA_PYTHON_VERSION:?DRA_PYTHON_VERSION is required}"
init_python

echo "--- :page_facing_up: Generating dependency CSV"
make install deps-csv

echo "Built artifacts:"
find dist -type f | sort
