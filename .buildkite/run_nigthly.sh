#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

MACHINE_TYPE=`uname -m`

if [ "$MACHINE_TYPE" != "x86_64" ] && [ -v SKIP_AARCH64 ]; then
  echo "Running on aarch64 and skipping"
  exit
fi


BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

# TODO to be moved in the image at https://github.com/elastic/ci-agent-images/blob/main/vm-images/enterprise-search/scripts/connectors-python/install-deps.sh#L6
sudo apt-get -y install liblz4-dev libunwind-dev

cd $ROOT

make install

export PIP=$ROOT/bin/pip

$PIP install py-spy
DATA_SIZE="${2:-small}"

# If we run on buildkite, we connect to docker so we can pull private images
# !!! WARNING be cautious about the following lines, to avoid leaking the secrets in the CI logs
set +x  # Do not remove so we don't leak passwords
if [ -v BUILDKITE ]; then
  echo "Connecting to Vault"
  VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
  VAULT_USER="docker-swiftypeadmin"
  echo "Fetching Docker credentials for '$VAULT_USER' from Vault..."
  DOCKER_USER=$(vault read -address "${VAULT_ADDR}" -field user_20230609 secret/ci/elastic-connectors-python/${VAULT_USER})
  DOCKER_PASSWORD=$(vault read -address "${VAULT_ADDR}" -field secret_20230609 secret/ci/elastic-connectors-python/${VAULT_USER})
  echo "Done!"

  # required by serverless
  sudo sysctl -w vm.max_map_count=262144
fi

PERF8=yes NAME=$1 DATA_SIZE=$DATA_SIZE make ftest
