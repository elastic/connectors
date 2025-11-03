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

VERSION_PATH="$PROJECT_ROOT/app/connectors_service/connectors/VERSION"
export VERSION=$(cat $VERSION_PATH)

# Variables for build.yaml
version="${VERSION}"
version_qualifier="${VERSION_QUALIFIER:-}"
revision="$(git rev-parse HEAD)"
repository="$(git config --get remote.origin.url)"

if [[ "${USE_SNAPSHOT:-}" == "true" ]]; then
  # SNAPSHOT workflows ignore version qualifier
  echo "Adding SNAPSHOT labeling"
  export VERSION="${VERSION}-SNAPSHOT"
  version="${VERSION}"
elif [[ -n "${VERSION_QUALIFIER:-}" ]]; then
  # version qualifier should not append version in build.yaml
  echo "Adding version qualifier labeling"
  version="${VERSION}-${VERSION_QUALIFIER}"
fi

# Create a build.yaml file for reference after build process
cat <<EOL > app/connectors_service/connectors/build.yaml
version: "$version"
qualifier: "$version_qualifier"
revision: "$revision"
repository: "$repository"
EOL

echo "Created app/connectors_service/connectors/build.yaml file:"
cat app/connectors_service/connectors/build.yaml

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
