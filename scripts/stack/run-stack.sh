#!/bin/bash

set -eo pipefail

export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if ! which docker-compose > /dev/null; then
    echo "Could not find 'docker-compose'. Make sure it is installed and available via your PATH"
    exit 2
fi

pushd "$CURDIR"

SECURE_STATE_DIR="$(mktemp -d)"
trap 'rm -rf -- "$SECURE_STATE_DIR"' EXIT

# Make sure ES_VERSION and ENT_VERSION are set
source $CURDIR/read-env.sh $CURDIR/.env
compose_file=$CURDIR/docker/docker-compose.yml
echo "Using compose file at: $compose_file"

. $CURDIR/parse-params.sh
parse_params $@
eval set -- "$parsed_params"

# for now, always update the images, make this an arg later
if [ "${update_images:-}" = true ]
then
  echo "Ensuring we have the latest images..."
  docker-compose -f $compose_file pull elasticsearch kibana elastic-connectors
fi

# create network
set +eo pipefail
NET_EXISTS=$(docker network ls | grep connectors_stack_net)
set -eo pipefail
if [[ "${NET_EXISTS:-}" != "" ]]; then
  echo "Docker network 'connectors_stack_net' already exists"
else
  echo "Creating network..."
  docker network create --attachable "connectors_stack_net"
fi

# Start Elasticsearch
echo "Starting Elasticsearch..."
docker-compose -f $compose_file up --detach elasticsearch
source $CURDIR/wait-for-elasticsearch.sh

# Start Kibana
echo "Starting Kibana..."
docker-compose -f $compose_file up --detach kibana
source $CURDIR/wait-for-kibana.sh
source $CURDIR/update-kibana-user-password.sh

run_configurator="no"
if [[ "${bypass_config:-}" == false ]]; then
  echo "Do you want to run the configurator?"
  select ync in "Yes" "No" "Cancel"; do
    case $ync in
      Yes ) run_configurator="yes"; break;;
      No ) break;;
      Cancel ) popd; exit 1;;
    esac
  done
  if [ $run_configurator == "yes" ]; then
    source ./configure-stack.sh $SECURE_STATE_DIR
  fi
fi

if [ "${no_connectors:-}" == false ]; then
  echo "Starting Elastic Connectors..."
  docker-compose -f $compose_file up --detach elastic-connectors
else
  echo "... Connectors service is set to not start... skipping..."
fi

echo "Stack is running. You can log in at http://localhost:5601/"

popd
