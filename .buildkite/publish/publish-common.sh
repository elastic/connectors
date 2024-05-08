#!/bin/bash

if [[ "${CURDIR:-}" == "" ]]; then
  echo "!! CURDIR is not set. Exiting."
  exit 2
fi

function realpath {
  echo "$(cd "$(dirname "$1")"; pwd)"/"$(basename "$1")";
}

export SCRIPT_DIR="$CURDIR"
export BUILDKITE_DIR=$(realpath "$(dirname "$SCRIPT_DIR")")
export PROJECT_ROOT=$(realpath "$(dirname "$BUILDKITE_DIR")")

VERSION_PATH="$PROJECT_ROOT/connectors/VERSION"
export VERSION=$(cat $VERSION_PATH)

if [[ "${USE_SNAPSHOT:-}" == "true" ]]; then
  echo "Adding SNAPSHOT labeling"
  export VERSION="${VERSION}-SNAPSHOT"
fi

export BASE_TAG_NAME=${DOCKER_IMAGE_NAME:-docker.elastic.co/enterprise-search/elastic-connectors}
export DOCKERFILE_PATH=${DOCKERFILE_PATH:-Dockerfile}
export DOCKER_ARTIFACT_KEY=${DOCKER_ARTIFACT_KEY:-elastic-connectors-docker-debian}

export VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
export VAULT_USER="docker-swiftypeadmin"
