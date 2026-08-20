#!/usr/bin/env bash
# Builds Python packages (whl + sdist) for connectors_service and connectors_sdk,
# and generates the dependency CSV using pip-licenses.
# Outputs are uploaded as Buildkite artifacts by the pipeline step.
set -euo pipefail

source .buildkite/shared.sh

PYTHON_VERSION="${DRA_PYTHON_VERSION:?DRA_PYTHON_VERSION is required}"
init_python

echo "--- :snake: Building Python packages"
cd app/connectors_service
make install-package
cd ../..

echo "--- :page_facing_up: Generating dependency CSV"
cd app/connectors_service
make deps-csv
cd ../..

echo "Built artifacts:"
find app/connectors_service/dist libs/connectors_sdk/dist -type f | sort
