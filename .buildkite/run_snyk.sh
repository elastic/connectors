#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python

make install

echo "--- Logging into snyk...---"
export SNYK_TOKEN=$(vault read -field=value secret/ci/elastic-connectors/SNYK_TOKEN)

echo "--- Downloading snyk..."
curl -sL --retry-max-time 60 --retry 3 --retry-delay 5 https://static.snyk.io/cli/latest/snyk-linux -o snyk
chmod +x ./snyk

echo "--- Running snyk for SDK..."
./snyk test --file=libs/connectors_sdk/requirements.txt --command=.venv/bin/python3 -- --allow-missing

echo "--- Running snyk for SDK..."
./snyk test --file=app/connectors_service/requirements.txt --command=.venv/bin/python3 -- --allow-missing
