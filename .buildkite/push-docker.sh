set -exu
set -o pipefail

function realpath {
  echo "$(cd "$(dirname "$1")"; pwd)"/"$(basename "$1")";
}

BUILDKITE_DIR=$(realpath "$(dirname "$0")")
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

echo "... placeholder ..."
