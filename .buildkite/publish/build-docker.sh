#!/bin/bash

set -exu
set -o pipefail

function realpath {
  echo "$(cd "$(dirname "$1")"; pwd)"/"$(basename "$1")";
}

SCRIPT_DIR=$(realpath "$(dirname "$0")")
BUILDKITE_DIR=$(realpath "$(dirname "$SCRIPT_DIR")")
PROJECT_ROOT=$(realpath "$(dirname "$BUILDKITE_DIR")")

if [[ "${ARCHITECTURE:-}" == "" ]]; then
  echo "!! ARCHITECTURE is not set. Exiting."
  exit 2
fi

VERSION_PATH="$PROJECT_ROOT/connectors/VERSION"
VERSION=$(cat $VERSION_PATH)

if [[ "${USE_SNAPSHOT:-}" == "true" ]]; then
  echo "Adding SNAPSHOT labeling"
  VERSION="${VERSION}-SNAPSHOT"
fi

pushd $PROJECT_ROOT
TAG_NAME="docker.elastic.co/enterprise-search/elastic-connectors-${ARCHITECTURE}:${VERSION}"
OUTPUT_PATH="$PROJECT_ROOT/.artifacts"
OUTPUT_FILE="$OUTPUT_PATH/elastic-connectors-docker-${VERSION}-${ARCHITECTURE}.tar.gz"

docker build -t $TAG_NAME .

mkdir -p $OUTPUT_PATH
docker save $TAG_NAME | gzip > $OUTPUT_FILE

popd
