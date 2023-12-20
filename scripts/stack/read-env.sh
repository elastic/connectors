#!/bin/bash

function realpath {
  echo "$(cd "$(dirname "$1")"; pwd)"/"$(basename "$1")";
}

if [[ "${CURDIR:-}" != "" && "${PROJECT_ROOT:-}" == "" ]]; then
  SCRIPT_DIR=$(realpath "$(dirname "$CURDIR")")
  export PROJECT_ROOT=$(realpath "$(dirname "$SCRIPT_DIR")")
  echo "set PROJECT_ROOT to $PROJECT_ROOT"
fi

# Fallback to .env file # if not already set
if [ -z "$ELASTICSEARCH_VERSION" ]
then
  export $(grep -E 'ELASTICSEARCH_VERSION' $1 | xargs)
fi
echo "ELASTICSEARCH_VERSION=$ELASTICSEARCH_VERSION"

if [ -z "$KIBANA_VERSION" ]
then
  export $(grep -E 'KIBANA_VERSION' $1 | xargs)
fi
echo "KIBANA_VERSION=$KIBANA_VERSION"

if [ -z "$CONNECTORS_VERSION" ]
then
  export $(grep -E 'CONNECTORS_VERSION' $1 | xargs)
fi
echo "CONNECTORS_VERSION=$CONNECTORS_VERSION"

if [ -z "$ELASTIC_PASSWORD" ]
then
  export ELASTIC_PASSWORD="changeme"
fi

uname_value=`uname`
case "${uname_value:none}" in
  Linux*) machine_os="Linux";;
  Darwin*) machine_os="MacOS";;
  FreeBSD*) machine_os="FreeBSD";;
  *) machine_os="UNKNOWN:${uname_value}"
esac
export MACHINE_OS="$machine_os"
