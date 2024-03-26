#!/bin/bash

########
# Pushes the docker image to the docker registry
########

set -exu
set -o pipefail

if [[ "${ARCHITECTURE:-}" == "" ]]; then
  echo "!! ARCHITECTURE is not set. Exiting."
  exit 2
fi

# Load our common environment variables for publishing
export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source $CURDIR/publish-common.sh

# Load the image from the artifact created in build-docker.sh
echo "Loading image from archive file..."
docker load < "$PROJECT_ROOT/.artifacts/elastic-connectors-docker-${VERSION}-${ARCHITECTURE}.tar.gz"

# ensure +x is set to avoid writing any sensitive information to the console
set +x

# Log into Docker
echo "Logging into docker..."
DOCKER_USER=$(vault read -address "${VAULT_ADDR}" -field user_20230609 secret/ci/elastic-connectors/${VAULT_USER})
vault read -address "${VAULT_ADDR}" -field secret_20230609 secret/ci/elastic-connectors/${VAULT_USER} | \
  docker login -u $DOCKER_USER --password-stdin docker.elastic.co

# Set our tag name and push the image
TAG_NAME="$BASE_TAG_NAME:${VERSION}-${ARCHITECTURE}"
echo "Pushing image to docker with tag: $TAG_NAME"
docker push $TAG_NAME
