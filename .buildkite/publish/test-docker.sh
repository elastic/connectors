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

arch_name=`uname -sr`

case "$arch_name" in
    Darwin*)
        echo "detected MacOS platform"
        LOCAL_MACHINE_ARCH="MacOS"
        ;;
    Linux*)
        echo "detected Linux platform"
        LOCAL_MACHINE_ARCH="Linux"
        ;;
    *)
        echo "Unsupported platform: $arch_name"
        exit 2
        ;;
esac

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
  if [[ "$LOCAL_MACHINE_ARCH" == "MacOS" ]]; then
    curl -LO https://storage.googleapis.com/container-structure-test/latest/container-structure-test-darwin-$ARCHITECTURE
    mv container-structure-test-darwin-$ARCHITECTURE container-structure-test
  else
    curl -LO https://storage.googleapis.com/container-structure-test/latest/container-structure-test-linux-$ARCHITECTURE
    mv container-structure-test-linux-$ARCHITECTURE container-structure-test
  fi

  chmod +x container-structure-test
  popd
fi

TEST_CONFIG_FILE="$PROJECT_ROOT/.buildkite/publish/container-structure-test.yaml"

if [[ -f "$TEST_CONFIG_FILE" ]]; then
  rm -f "$TEST_CONFIG_FILE"
fi

ESCAPED_VERSION=${VERSION//./\\\\.}

TEST_CONFIG_TEXT='
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
    expectedOutput: ["'"${ESCAPED_VERSION}"'*"]
'

printf '%s\n' "$TEST_CONFIG_TEXT" > "$TEST_CONFIG_FILE"

echo "Running container-structure-test"
"$TEST_EXEC" test --image "$TAG_NAME" --config "$TEST_CONFIG_FILE"
