#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python
cd "$PACKAGE_PATH"
python -m pip install --upgrade build twine
python -m build

export TWINE_USERNAME="__token__"

# upload to test or real PyPI based on TEST_PYPI=1 env var or arg
if [[ "${1:-}" == "TEST_PYPI=1" ]] || [[ "${TEST_PYPI:-}" =~ ^(1|TRUE|true)$ ]]; then
  TWINE_PASSWORD=$(vault read -field publishing-api-key secret/ci/elastic-connectors/test-pypi)
  export TWINE_PASSWORD
  python -m twine upload --repository testpypi dist/*
else
  TWINE_PASSWORD=$(vault read -field publishing-api-key secret/ci/elastic-connectors/pypi)
  export TWINE_PASSWORD
  python -m twine upload --repository pypi dist/*
fi
