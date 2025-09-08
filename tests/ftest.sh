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
# Retry configuration
CURL_MAX_RETRIES=3
CURL_RETRY_DELAY=5

export DOCKERFILE_FTEST_PATH=${DOCKERFILE_FTEST_PATH:-tests/Dockerfile.ftest}
export PERF8_TRACE=${PERF8_TRACE:-False}
export REFRESH_RATE="${REFRESH_RATE:-5}"
export DATA_SIZE="${DATA_SIZE:-medium}"
export RUNNING_FTEST=True
export VERSION="${CONNECTORS_VERSION}-SNAPSHOT"




# Download and load ES Docker images from DRA artifacts instead of relying on the snapshot image in the registry.
# This is needed for the release process when the ES snapshot image may not yet be available.
# Snapshot images are pushed to the registry by the unified release workflow.

# Determine system architecture
ARCH=$(uname -m)
if [[ $ARCH == "arm64" ]]; then
  ARCH="aarch64"
fi

# Function to check if Docker image exists locally
function check_local_image {
  local version=$1
  local arch=$2
  local image_name="elasticsearch:${version}-${arch}"

  if docker image inspect "$image_name" >/dev/null 2>&1; then
    echo "$image_name"
    return 0
  fi
  return 1
}

# Function to generate fallback versions for patch
function get_fallback_versions {
  local version=$1
  local version_without_snapshot=${version%-SNAPSHOT}

  # Extract major.minor.patch components
  local major=$(echo "$version_without_snapshot" | cut -d. -f1)
  local minor=$(echo "$version_without_snapshot" | cut -d. -f2)
  local patch=$(echo "$version_without_snapshot" | cut -d. -f3)

  local fallback_versions=()

  # Try current patch version first
  fallback_versions+=("${major}.${minor}.${patch}-SNAPSHOT")

  # Try only n-1 (previous patch version)
  if [ "$patch" -gt 0 ]; then
    fallback_versions+=("${major}.${minor}.$((patch-1))-SNAPSHOT")
  fi

  echo "${fallback_versions[@]}"
}

# Function to resolve the DRA manifest URL
function resolve_dra_manifest {
  local dra_artifact=$1
  local dra_version=$2

  local response=$(curl -sS -f --retry $CURL_MAX_RETRIES --retry-delay $CURL_RETRY_DELAY --retry-connrefused "$ARTIFACT_BASE_URL/$dra_artifact/latest/$dra_version.json" 2>&1)
  local curl_exit_code=$?

  if [ $curl_exit_code -ne 0 ]; then
    echo "Error: Failed to fetch DRA manifest for artifact $dra_artifact and version $dra_version." >&2
    echo "Details: $response" >&2
    return 1
  fi

  local manifest_url=$(echo "$response" | jq -r '.manifest_url' 2>/dev/null)

  if [ -z "$manifest_url" ] || [ "$manifest_url" == "null" ]; then
    echo "Error: No manifest_url found in the response for artifact $dra_artifact and version $dra_version." >&2
    echo "Response: $response" >&2
    return 1
  fi

  echo "$manifest_url"
}

# Function to get Docker tarball URL from manifest
function get_docker_tarball_url {
  local manifest_url=$1
  local tarball_name=$2

  local tarball_url=$(curl -sS --retry $CURL_MAX_RETRIES --retry-delay $CURL_RETRY_DELAY --retry-connrefused "$manifest_url" | jq -r ".projects.elasticsearch.packages.\"$tarball_name\".url")

  if [ -z "$tarball_url" ] || [ "$tarball_url" == "null" ]; then
    return 1
  fi

  echo "$tarball_url"
}

# Function to download the Docker image tarball
function download_docker_tarball {
  local tar_url=$1
  local tar_file=$2

  echo "Downloading Docker image tarball from $tar_url..."
  curl --http1.1 --retry $CURL_MAX_RETRIES --retry-delay $CURL_RETRY_DELAY --retry-connrefused -O "$tar_url"

  if [ ! -f "$tar_file" ]; then
    echo "Error: Download failed. File $tar_file not found." >&2
    return 1
  fi
}

# Function to load the Docker image directly from the tarball
function load_docker_image {
  local tar_file=$1
  local version=$2
  local arch=$3

  echo "Loading Docker image from $tar_file..."
  docker load < "$tar_file"

  # Tag the image with version and arch for future reference
  local loaded_image=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "elasticsearch" | head -1)
  if [ -n "$loaded_image" ]; then
    docker tag "$loaded_image" "elasticsearch:${version}-${arch}"
    echo "Tagged image as elasticsearch:${version}-${arch}"
  fi
}

# Function to get Docker tarball name based on architecture
function get_docker_tarball_name {
  local version=$1
  local arch=$2

  case $arch in
    x86_64)
      echo "elasticsearch-${version}-docker-image-amd64.tar.gz"
      ;;
    aarch64)
      echo "elasticsearch-${version}-docker-image-arm64.tar.gz"
      ;;
    *)
      echo "Error: Unsupported architecture $arch" >&2
      return 1
      ;;
  esac
}

# Function to fetch and load Docker image with fallback logic
function fetch_elasticsearch_image {
  local target_version=$VERSION
  local fallback_versions=($(get_fallback_versions "$target_version"))

  # Check if exact version exists locally first
  if local_image=$(check_local_image "$target_version" "$ARCH"); then
    echo "Found local image: $local_image"
    export ELASTICSEARCH_DRA_DOCKER_IMAGE="$local_image"
    return 0
  fi

  # Try to fetch from registry with fallback
  for version in "${fallback_versions[@]}"; do
    echo "Attempting to fetch Elasticsearch version: $version"

    local tarball_name=$(get_docker_tarball_name "$version" "$ARCH")
    if [ $? -ne 0 ]; then
      continue
    fi

    local manifest_url=$(resolve_dra_manifest "elasticsearch" "$version")
    if [ $? -ne 0 ]; then
      echo "Failed to resolve manifest for version $version, trying next..." >&2
      continue
    fi

    local tarball_url=$(get_docker_tarball_url "$manifest_url" "$tarball_name")
    if [ $? -ne 0 ]; then
      echo "Failed to get tarball URL for version $version, trying next..." >&2
      continue
    fi

    # Download and load the image
    if download_docker_tarball "$tarball_url" "$tarball_name" && \
       load_docker_image "$tarball_name" "$version" "$ARCH"; then

      export ELASTICSEARCH_DRA_DOCKER_IMAGE="elasticsearch:${version}-${ARCH}"
      echo "Successfully loaded Elasticsearch $version. Image name: $ELASTICSEARCH_DRA_DOCKER_IMAGE"

      # Clean up
      rm -f "$tarball_name"
      return 0
    fi

    echo "Failed to download/load version $version, trying next..." >&2
  done

  echo "Error: Failed to fetch any compatible Elasticsearch version" >&2
  return 1
}

# Main execution
echo "Fetching Elasticsearch Docker image for version $VERSION on $ARCH architecture..."
if fetch_elasticsearch_image; then
  echo "Docker image setup completed successfully"
else
  exit 1
fi

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
