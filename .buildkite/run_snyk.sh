#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)
APP_ROOT=$ROOT/app/connectors_service
SDK_ROOT=$ROOT/libs/connectors_sdk


init_python

make install freeze

echo "--- Logging into snyk...---"
export SNYK_TOKEN=$(vault read -field=value secret/ci/elastic-connectors/SNYK_TOKEN)

echo "--- Downloading snyk..."
curl -sL --retry-max-time 60 --retry 3 --retry-delay 5 https://static.snyk.io/cli/latest/snyk-linux -o snyk
chmod +x ./snyk

echo "--- Initializing venv for the test ---"
python3 -m .snyk-venv
echo "--- Installing dependencies ---"
$ROOT/.snyk-venv/bin/pip install \
        -r $SDK_ROOT/requirements.txt \
        -r $APP_ROOT/requirements.txt

echo "--- Running snyk for SDK..."
./snyk test --file=$SDK_ROOT/requirements.txt --command=$ROOT/.venv/bin/python3

echo "--- Running snyk for App..."
./snyk test --file=$APP_ROOT/requirements.txt --command=$ROOT/.venv/bin/python3
