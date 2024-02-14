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

echo "Ensuring test environment is set up"

BIN_DIR="$PROJECT_ROOT/bin"
TEST_EXEC="$BIN_DIR/container-structure-test"
if [[ ! -f "$TEST_EXEC" ]]; then
  mkdir -p "$BIN_DIR"

  pushd "$BIN_DIR"
  curl -LO https://storage.googleapis.com/container-structure-test/latest/container-structure-test-linux-$ARCHITECTURE
  mv container-structure-test-linux-$ARCHITECTURE container-structure-test
  chmod +x container-structure-test
  popd
fi

TEST_CONFIG="$PROJECT_ROOT/.buildkite/publish/container-structure-test.yaml"

if [[ -f "$TEST_CONFIG" ]]; then
  rm -f "$TEST_CONFIG"
fi

ESCAPED_VERSION=${$VERSION//./\\\\.}

read -r -d '' TEST_CONFIG_TEXT <<-EOF_TEST_CONFIG
schemaVersion: "2.0.0"

commandTests:
  # ensure Python 3.10.* is installed
  - name: "Python 3 Installation 3.10.*"
    command: "python3"
    args: ["--version"]
    expectedOutput: ["Python\\s3\\.10\\.*"]
  - name: "Connectors Installation"
    command: "/app/bin/elastic-ingest"
    args: ["--version"]
    expectedOutput: ["${ESCAPED_VERSION}*"]
EOF_TEST_CONFIG

cat "$TEST_CONFIG_TEXT" > "$TEST_CONFIG"

echo "Running container-structure-test"
"$TEST_EXEC" test --image "$TAG_NAME" --config "$TEST_CONFIG"
