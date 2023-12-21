#!/bin/bash

if [[ "${CURDIR:-}" == "" ]]; then
  export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi

function realpath {
  echo "$(cd "$(dirname "$1")"; pwd)"/"$(basename "$1")";
}

if [[ "${CURDIR:-}" != "" && "${PROJECT_ROOT:-}" == "" ]]; then
  SCRIPT_DIR=$(realpath "$(dirname "$CURDIR")")
  export PROJECT_ROOT=$(realpath "$(dirname "$SCRIPT_DIR")")
  echo "set PROJECT_ROOT to $PROJECT_ROOT"
fi

if [[ "${CONNECTORS_VERSION:-}" == "" ]]; then
  SET_CONNECTORS_VERSION=`head -1 $PROJECT_ROOT/connectors/VERSION`
else
  SET_CONNECTORS_VERSION="$CONNECTORS_VERSION"
fi

SET_STACK_VERSION=`echo "$SET_CONNECTORS_VERSION" | sed 's/\.[0-9]$//'`

if [ "$use_snapshot" == true ]; then
  SET_CONNECTORS_VERSION="$SET_CONNECTORS_VERSION-SNAPSHOT"
  SET_STACK_VERSION="$SET_STACK_VERSION-SNAPSHOT"
fi

if [ -z "$ELASTICSEARCH_VERSION" ]
then
  export ELASTICSEARCH_VERSION="$SET_STACK_VERSION"
fi
echo "ELASTICSEARCH_VERSION=$ELASTICSEARCH_VERSION"

if [ -z "$KIBANA_VERSION" ]
then
  export KIBANA_VERSION="$SET_STACK_VERSION"
fi
echo "KIBANA_VERSION=$KIBANA_VERSION"

if [ -z "$CONNECTORS_VERSION" ]
then
  export CONNECTORS_VERSION="$SET_CONNECTORS_VERSION"
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
