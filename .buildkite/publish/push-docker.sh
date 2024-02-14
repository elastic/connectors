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

TAG_NAME="docker.elastic.co/enterprise-search/elastic-connectors-${ARCHITECTURE}:${VERSION}"

echo "Loading image from archive file..."
docker load < "$PROJECT_ROOT/.artifacts/elastic-connectors-docker-${VERSION}-${ARCHITECTURE}.tar.gz"

VAULT_ADDR=${VAULT_ADDR:-https://vault-ci-prod.elastic.dev}
VAULT_USER="docker-swiftypeadmin"
DOCKER_USER=$(vault read -address "${VAULT_ADDR}" -field user_20230609 secret/ci/elastic-connectors/${VAULT_USER})
DOCKER_PASSWORD=$(vault read -address "${VAULT_ADDR}" -field secret_20230609 secret/ci/elastic-connectors/${VAULT_USER})

echo "Logging into docker..."
docker login -u $DOCKER_USER -p $DOCKER_PASSWORD docker.elastic.co

echo "Pushing image to docker with tag: $TAG_NAME"
docker push $TAG_NAME
