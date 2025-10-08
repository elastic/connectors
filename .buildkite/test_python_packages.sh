#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python

python -m pip install --upgrade build twine
python -m build "$PACKAGE_PATH"
ls -lah "$PACKAGE_PATH/dist/"
python -m twine check "$PACKAGE_PATH/dist/*"

# If this is the connectors_service package, test the installation and CLI
if [[ "$PACKAGE_PATH" == *app/connectors_service* ]]; then
  echo "Testing connectors_service package installation..."

  # Install the connectors_sdk package first
  LIB_PATH="libs/connectors_sdk"
  python -m build "$LIB_PATH"
  python -m pip install "$LIB_PATH"/dist/*.whl

  python -m pip install "$PACKAGE_PATH"/dist/*.whl
  connectors --help
  elastic-ingest --help
#  elastic-agent-connectors --help
  test-connectors --help
fi
