#!/bin/bash

set -eo pipefail

export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if ! which docker-compose > /dev/null; then
    echo "Could not find 'docker-compose'. Make sure it is installed and available via your PATH"
    exit 2
fi

pushd "$CURDIR"

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

if [[ "${connectors_only}" != true ]]; then
  # Start Elasticsearch
  echo "Starting Elasticsearch..."
  docker-compose -f $compose_file up --detach elasticsearch
  source $CURDIR/wait-for-elasticsearch.sh

  # Start Kibana
  echo "Starting Kibana..."
  docker-compose -f $compose_file up --detach kibana
  source $CURDIR/wait-for-kibana.sh
  source $CURDIR/update-kibana-user-password.sh
fi

run_configurator="no"
if [[ "${bypass_config:-}" == false ]]; then
  while true; do
    read -p "Do you want to run the configurator? (y/n) " yn
    case $yn in
      [yY] ) run_configurator="yes"; break;;
      [nN] ) break;;
      * ) echo "invalid response";;
    esac
  done
  if [ $run_configurator == "yes" ]; then
    source ./copy-config.sh
    source ./configure-connectors.sh $SECURE_STATE_DIR
  fi
fi

if [ "${no_connectors:-}" == false ]; then
  echo "Starting Elastic Connectors..."

  config_dir="$PROJECT_ROOT/scripts/stack/connectors-config"
  script_config="$config_dir/config.yml"
  if [ ! -f "$script_config" ]; then
    echo "Could not find a connectors configuration at: $script_config"
    echo "Exiting..."
    exit 2
  fi

  docker-compose -f $compose_file up --detach elastic-connectors
else
  echo "... Connectors service is set to not start... skipping..."
fi

echo "Stack is running. You can log in at http://localhost:5601/"

popd
