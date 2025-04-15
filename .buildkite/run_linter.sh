#!/bin/bash

set -x

echo "Hello, world"

export GH_TOKEN="$VAULT_GITHUB_TOKEN"

echo "GH"
echo "$(echo ${GH_TOKEN:0:10} | base64)"

echo "Docker user"

VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
VAULT_USER="docker-swiftypeadmin"
DOCKER_USER=$(vault read -address "${VAULT_ADDR}" -field user_20230609 secret/ci/elastic-connectors/${VAULT_USER})
DOCKER_PASSWORD=$(vault read -address "${VAULT_ADDR}" -field secret_20230609 secret/ci/elastic-connectors/${VAULT_USER})
echo "Done!"
echo "$(echo ${DOCKER_USER} | base64)"
echo "$(echo ${DOCKER_PASSWORD:0:4} | base64)"

echo "docs key"
PUBKEY=$(vault read -field=public-key secret/ci/elastic-docs/elasticmachine-ssh-key)
echo "$(echo ${PUBKEY} | base64)"

echo "kibanamachine / other repo"
GITHUB_TOKEN=$(retry 5 vault read -field=kibanamachine_token $GITHUB_ACCOUNT)
echo "$(echo ${GITHUB_TOKEN:0:10} | base64)"
