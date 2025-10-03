#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python

cd "$PACKAGE_PATH"
python -m pip install --upgrade build twine
python -m build
ls -lah dist/
python -m twine check dist/*
