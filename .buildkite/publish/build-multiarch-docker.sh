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

BASE_TAG_NAME="docker.elastic.co/enterprise-search/elastic-connectors"
TAG_NAME="${BASE_TAG_NAME}:${VERSION}"
AMD64_TAG="${BASE_TAG_NAME}-amd64:${VERSION}"
ARM64_TAG="${BASE_TAG_NAME}-arm64:${VERSION}"

VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
VAULT_USER="docker-swiftypeadmin"
DOCKER_USER=$(vault read -address "${VAULT_ADDR}" -field user_20230609 secret/ci/elastic-connectors/${VAULT_USER})
DOCKER_PASSWORD=$(vault read -address "${VAULT_ADDR}" -field secret_20230609 secret/ci/elastic-connectors/${VAULT_USER})

buildah login --username="${DOCKER_USER}" --password="${DOCKER_PASSWORD}" docker.elastic.co

buildah manifest create $TAG_NAME \
  $AMD64_TAG \
  $ARM64_TAG

buildah manifest push $TAG_NAME docker://$TAG_NAME

echo "Built and pushed multiarch image... dumping manifest..."
buildah manifest inspect $TAG_NAME
