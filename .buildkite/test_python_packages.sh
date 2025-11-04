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
  echo "Testing connectors_service package installation and CLI..."

  # Install the connectors_sdk package first
  LIB_PATH="libs/connectors_sdk"
  python -m build "$LIB_PATH"
  python -m pip install "$LIB_PATH"/dist/*.whl

  python -m pip install "$PACKAGE_PATH"/dist/*.whl
  connectors --help
  elastic-ingest --help
#  elastic-agent-connectors --help
  test-connectors --help
else
  python -m pip install "$PACKAGE_PATH"/dist/*.whl
  python -c "import connectors_sdk; print(f'🎉 Success! connectors_sdk version: {connectors_sdk.__version__}')"
fi

if [[ "${PYTHON_VERSION:-}" == "${DRA_PYTHON_VERSION:-}" ]]; then
  if [[ "$PACKAGE_PATH" == *app/connectors_service* ]]; then
    buildkite-agent artifact upload 'app/connectors_service/dist/*.whl'
    buildkite-agent artifact upload 'app/connectors_service/dist/*.tar.gz'
  elif [[ "$PACKAGE_PATH" == *libs/connectors_sdk* ]]; then
    buildkite-agent artifact upload 'libs/connectors_sdk/dist/*.whl'
    buildkite-agent artifact upload 'libs/connectors_sdk/dist/*.tar.gz'
  fi
fi
