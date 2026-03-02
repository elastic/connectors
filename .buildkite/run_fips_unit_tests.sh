#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

echo "=== Loading pre-built FIPS Docker image ==="
mkdir -p .artifacts
buildkite-agent artifact download '.artifacts/connectors-fips-base.tar.gz' .artifacts/ --step build_fips_image
docker load < .artifacts/connectors-fips-base.tar.gz
rm -f .artifacts/connectors-fips-base.tar.gz

make fips-test
