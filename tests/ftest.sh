#!/bin/bash
set -exu
set -o pipefail

NAME=$1
PERF8=$2

SERVICE_TYPE=${NAME%"_serverless"}
INDEX_NAME=search-${NAME%"_serverless"}

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ROOT_DIR="$SCRIPT_DIR/.."
VIRTUAL_ENV="$ROOT_DIR/.venv"
PLATFORM='unknown'
MAX_RSS="200M"
MAX_DURATION=600
CONNECTORS_VERSION=$(cat "$ROOT_DIR/connectors/VERSION")
ARTIFACT_BASE_URL="https://artifacts-snapshot.elastic.co"

export DOCKERFILE_FTEST_PATH=${DOCKERFILE_FTEST_PATH:-tests/Dockerfile.ftest}
export PERF8_TRACE=${PERF8_TRACE:-False}
export REFRESH_RATE="${REFRESH_RATE:-5}"
export DATA_SIZE="${DATA_SIZE:-medium}"
export RUNNING_FTEST=True
export VERSION="${CONNECTORS_VERSION}-SNAPSHOT"

# Download and load ES Docker images from DRA artifacts instead of relying on the snapshot image in the registry.
# This is needed for the release process when the ES snapshot image may not yet be available.
# Snapshot images are pushed to the registry by the unified release workflow.

# Function to resolve the DRA manifest URL
function resolve_dra_manifest {
  DRA_ARTIFACT=$1
  DRA_VERSION=$2

  # Perform the curl request and capture the output and exit code
  RESPONSE=$(curl -sS -f $ARTIFACT_BASE_URL/$DRA_ARTIFACT/latest/$DRA_VERSION.json 2>&1)
  CURL_EXIT_CODE=$?

  # Check if the curl command failed
  if [ $CURL_EXIT_CODE -ne 0 ]; then
    echo "Error: Failed to fetch DRA manifest for artifact $DRA_ARTIFACT and version $DRA_VERSION."
    echo "Details: $RESPONSE"
    exit 1
  fi

  # Extract the manifest_url using jq
  MANIFEST_URL=$(echo "$RESPONSE" | jq -r '.manifest_url' 2>/dev/null)

  # Check if the jq command succeeded and if manifest_url is non-empty
  if [ -z "$MANIFEST_URL" ] || [ "$MANIFEST_URL" == "null" ]; then
    echo "Error: No manifest_url found in the response for artifact $DRA_ARTIFACT and version $DRA_VERSION."
    echo "Response: $RESPONSE"
    exit 1
  fi

  # Output the manifest URL
  echo "$MANIFEST_URL"
}

# Function to download the Docker image tarball
function download_docker_tarball {
  TAR_URL=$1
  TAR_FILE=$2
  MAX_RETRIES=3

  echo "Downloading Docker image tarball from $TAR_URL..."
  curl --http1.1 --retry $MAX_RETRIES --retry-connrefused -O "$TAR_URL"

  if [ ! -f "$TAR_FILE" ]; then
    echo "Error: Download failed. File $TAR_FILE not found."
    exit 1
  fi
}

# Function to load the Docker image directly from the tarball
function load_docker_image {
  TAR_FILE=$1
  echo "Loading Docker image from $TAR_FILE..."
  docker load < "$TAR_FILE"
}

# Determine system architecture
ARCH=$(uname -m)

# Normalize architecture name
if [[ $ARCH == "arm64" ]]; then
  ARCH="aarch64"
fi

# Select the appropriate Docker tarball name based on architecture
case $ARCH in
  x86_64)
    DOCKER_TARBALL_NAME="elasticsearch-$VERSION-docker-image-amd64.tar.gz"
    ;;
  aarch64)
    DOCKER_TARBALL_NAME="elasticsearch-$VERSION-docker-image-arm64.tar.gz"
    ;;
  *)
    echo "Error: Unsupported architecture $ARCH"
    exit 1
    ;;
esac

# Get the DRA manifest URL for Elasticsearch
ELASTICSEARCH_DRA_MANIFEST=$(resolve_dra_manifest "elasticsearch" $VERSION)

# Parse Docker image tarball information
DOCKER_TARBALL_URL=$(curl -sS "$ELASTICSEARCH_DRA_MANIFEST" | jq -r ".projects.elasticsearch.packages.\"$DOCKER_TARBALL_NAME\".url")

if [ -z "$DOCKER_TARBALL_URL" ] || [ "$DOCKER_TARBALL_URL" == "null" ]; then
  echo "Error: Docker tarball URL not found in the manifest."
  exit 1
fi

# Execute the functions to download and load the image
download_docker_tarball "$DOCKER_TARBALL_URL" "$DOCKER_TARBALL_NAME"
load_docker_image "$DOCKER_TARBALL_NAME"

# Export image name following DRA conventions
export ELASTICSEARCH_DRA_DOCKER_IMAGE="elasticsearch:$ARCH"

echo "Docker image for Elasticsearch $VERSION loaded successfully. Image name: $ELASTICSEARCH_DRA_DOCKER_IMAGE"

echo "Cleaning up the downloaded tarball..."
rm -f "$DOCKER_TARBALL_NAME"

if [ "$PERF8_TRACE" == true ]; then
    echo 'Tracing is enabled, memray stats will be delivered'
    PLUGINS='--asyncstats --memray --psutil'
else
    PLUGINS='--asyncstats --psutil'
fi

PERF8_BIN=${PERF8_BIN:-$VIRTUAL_ENV/bin/perf8}
PYTHON=${PYTHON:-$VIRTUAL_ENV/bin/python}
ELASTIC_INGEST=${ELASTIC_INGEST:-$VIRTUAL_ENV/bin/elastic-ingest}

unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   PLATFORM='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   PLATFORM='darwin'
fi


cd $ROOT_DIR/tests/sources/fixtures

if [ -f "$NAME/.env" ]; then
  echo "Loading env for $NAME"
  export $(grep -v '^#' $NAME/.env | xargs)
fi


if [ -f "$NAME/requirements.txt" ]; then
$PYTHON -m pip install -r $NAME/requirements.txt
fi
$PYTHON fixture.py --name $NAME --action setup
$PYTHON fixture.py --name $NAME --action start_stack
$PYTHON fixture.py --name $NAME --action check_stack
$VIRTUAL_ENV/bin/fake-kibana --index-name $INDEX_NAME --service-type $SERVICE_TYPE --config-file $NAME/config.yml --connector-definition $NAME/connector.json --debug
$PYTHON fixture.py --name $NAME --action load

if [[ $PERF8 == "yes" ]]
then
    $PYTHON fixture.py --name $NAME --action description > description.txt
    if [[ $PLATFORM == "darwin" ]]
    then
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME $PLUGINS --max-duration $MAX_DURATION --description description.txt -c $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
    else
      $PERF8_BIN --refresh-rate $REFRESH_RATE -t $ROOT_DIR/perf8-report-$NAME $PLUGINS --max-duration $MAX_DURATION --description description.txt -c $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
    fi
else
    $ELASTIC_INGEST --config-file $NAME/config.yml --debug & PID=$!
fi


$PYTHON fixture.py --name $NAME --action monitor --pid $PID

$PYTHON fixture.py --name $NAME --action remove

$ELASTIC_INGEST  --config-file $NAME/config.yml  --debug & PID_2=$!

$PYTHON fixture.py --name $NAME --action monitor --pid $PID_2


NUM_DOCS=`$PYTHON fixture.py --name $NAME --action get_num_docs`
$PYTHON $ROOT_DIR/scripts/verify.py --index-name $INDEX_NAME --service-type $NAME --size $NUM_DOCS
$PYTHON fixture.py --name $NAME --action teardown

# stopping the stack as a final step once everything else is done.
$PYTHON fixture.py --name $NAME --action stop_stack

# Wait for PERF8 to compile the report
# Actual report compilation starts right when the first sync finishes, but happens in the background
# So we wait in the end of the script to not block second sync from happening while we also compile the report
if [[ $PERF8 == "yes" ]]; then
    set +e
    PERF8_PID=`ps aux | grep bin/perf8 | grep -v grep | awk '{print $2}'`
    if [ ! -z "$PERF8_PID" ] # if the process is already gone, move on
    then
      echo 'Waiting for PERF8 to finish the report'
      while kill -0 "$PERF8_PID"; do
          sleep 0.5
      done
    fi
    set -e

    rm -f description.txt

    # reading the status to know if we need to fail
    STATUS=$(<$ROOT_DIR/perf8-report-$NAME/status)
    exit $STATUS
fi

# make sure the ingest processes are terminated
set +e # if the PID disappears right before the kill, that's not an error
if ps -p $PID > /dev/null
then
  echo 'Killing the ingest process'
  kill -TERM $PID
  sleep 5
  if ps -p $PID > /dev/null
  then
    kill -KILL $PID
  fi
fi

if ps -p $PID_2 > /dev/null
then
  echo 'Killing the second ingest process'
  kill -TERM $PID_2
  sleep 5
  if ps -p $PID_2 > /dev/null
  then
    kill -KILL $PID_2
  fi
fi
set -e
