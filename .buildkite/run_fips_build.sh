#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

echo "=== Building FIPS Docker images ==="

make fips-build-test

echo "=== Saving FIPS images as artifacts ==="
mkdir -p .artifacts
docker save connectors-fips-base | gzip > .artifacts/connectors-fips-base.tar.gz
docker save connectors-fips-test | gzip > .artifacts/connectors-fips-test.tar.gz

echo "=== FIPS images built and saved ==="
docker images connectors-fips-base
docker images connectors-fips-test
ls -lh .artifacts/connectors-fips-*.tar.gz
