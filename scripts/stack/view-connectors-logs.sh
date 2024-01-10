#!/bin/bash

set -eo pipefail

export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
if ! which docker-compose > /dev/null; then
    echo "Could not find 'docker-compose'. Make sure it is installed and available via your PATH"
    exit 2
fi

pushd "$CURDIR"

compose_file=$CURDIR/docker/docker-compose.yml
echo "Using compose file at: $compose_file"

. $CURDIR/parse-params.sh
parse_params $@
eval set -- "$parsed_params"

source $CURDIR/set-env.sh $CURDIR/.env

if [ "${watch_logs:-}" = true ]
then
    docker-compose -f "$compose_file" logs -f elastic-connectors
else
    docker-compose -f "$compose_file" logs -n20 elastic-connectors
fi
