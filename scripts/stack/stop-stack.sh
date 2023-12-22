#!/bin/bash

set -eo pipefail

export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if ! which docker-compose > /dev/null; then
    echo "Could not find 'docker-compose'. Make sure it is installed and available via your PATH"
    exit 2
fi

source $CURDIR/read-env.sh $CURDIR/.env
compose_file=$CURDIR/docker/docker-compose.yml

. $CURDIR/parse-params.sh
parse_params $@
eval set -- "$parsed_params"

echo "Stopping running containers..."
if [ "${remove_volumes:-}" == true ]; then
  echo ".. also removing data volumes..."
  docker-compose -f $compose_file down -v
else
  docker-compose -f $compose_file down
fi

echo "Removing network..."
docker network rm "connectors_stack_net"
