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

source $SCRIPT_DIR/git-setup.sh

VERSION_PATH="$PROJECT_ROOT/connectors/VERSION"
export VERSION=$(cat $VERSION_PATH)

if [[ "${USE_SNAPSHOT:-}" == "true" ]]; then
  echo "Adding √SNAPSHOT labeling"
  export VERSION="${VERSION}-SNAPSHOT"
elif [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  echo "Adding version qualifier labeling"
  # SNAPSHOT workflows ignore version qualifier
  export VERSION="${VERSION}-${VERSION_QUALIFIER}"
fi

REVISION="$(git rev-parse HEAD)"
REPOSITORY="$(git config --get remote.origin.url)"

# Create a build.properties file for reference during build process
cat <<EOL > build.properties
version=$VERSION
qualifier=$VERSION_QUALIFIER
revision=$REVISION
repository=$REPOSITORY
EOL

echo "Created build.properties file:"
cat build.properties

if [[ "${MANUAL_RELEASE:-}" == "true" ]]; then
  # This block is for out-of-band releases, triggered by the release-pipeline
  # See discussion in https://github.com/elastic/connectors/pull/2804/commits/d27e4c18650bc2dfd099018080fe16ad307eb18b#r1758850508
  # See also RELEASING.md
  export ORIG_VERSION=$(buildkite-agent meta-data get orig_version)
  IFS='.' read -ra VERSION_PARTS <<< "$ORIG_VERSION"
  PATCH_PART=${VERSION_PARTS[2]}
  LAST_PATCH="$((PATCH_PART - 1))"
  LAST_VERSION="${VERSION_PARTS[0]}.${VERSION_PARTS[1]}.${LAST_PATCH}"
  echo "last version was ${LAST_VERSION}"
  echo "Adding timestamp version suffix"
  VERSION_SUFFIX=$(buildkite-agent meta-data get timestamp)
  export DOCKER_TAG_VERSION="${LAST_VERSION}.build$VERSION_SUFFIX"
  export VERSION="${LAST_VERSION}+build$VERSION_SUFFIX"
else
  export DOCKER_TAG_VERSION=${VERSION}
fi

export BASE_TAG_NAME=${DOCKER_IMAGE_NAME:-docker.elastic.co/integrations/elastic-connectors}
export DOCKERFILE_PATH=${DOCKERFILE_PATH:-Dockerfile}
export PROJECT_NAME=${PROJECT_NAME:-elastic-connectors}
export DOCKER_ARTIFACT_KEY=${DOCKER_ARTIFACT_KEY:-${PROJECT_NAME}-docker}
export VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
export VAULT_USER="docker-swiftypeadmin"
