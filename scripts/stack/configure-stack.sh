#!/bin/bash

set -o pipefail

OUTPUT_DIR=${1:-}
if [[ ${OUTPUT_DIR:-} == "" ]]; then
    echo "Missing argument 1 for OUTPUT_DIR"
    exit 2
fi

if [[ ${SECURE_STATE_DIR:-} == "" ]]; then
    export SECURE_STATE_DIR="$(mktemp -d)"
    trap 'rm -rf -- "$SECURE_STATE_DIR"' EXIT
fi

if [[ ${CURDIR:-} == "" ]]; then
    export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi
source $CURDIR/read-env.sh $CURDIR/.env

PYTHON_EXECUTABLE=""

if which python3 > /dev/null; then
    PY_VERSION=`python3 --version`
    # TODO - if we're OK - set the executable
    PYTHON_EXECUTABLE="python3"
fi

if [ $PYTHON_EXECUTABLE == "" ]; then
    if which python > /dev/null; then
        PY_VERSION=`python --version`
        PYTHON_EXECUTABLE="python"
    fi
fi

if [ $PYTHON_EXECUTABLE == "" ]; then
    echo "Could not find a suitable Python 3 executable..."
    exit 2
fi

pushd $PROJECT_ROOT
    CONFIG_FILE="${PROJECT_ROOT}/scripts/stack/connectors-config/config.yml"
    CONNECTORS_EXE="${PROJECT_ROOT}/bin/connectors"
    if [ ! -f "$CONNECTORS_EXE" ]; then
        echo "Could not find a connectors executable, running 'make clean install'"
        make clean install PYTHON=$PYTHON_EXECUTABLE
    fi

    keep_configuring=true
    while [ $keep_configuring == true ]; do
        echo
        echo "Currently configured connectors:"
        $CONNECTORS_EXE --config "$CONFIG_FILE" connector list
        echo
        while true; do
            read -p "Do you want to set up a new connector? (y/N) " yn
            case $yn in
                [yY] ) break;;
                [nN] ) keep_configuring=false; break;;
                * ) keep_configuring=false; break;;
            esac
        done

        if [ $keep_configuring == true ]; then
            $CONNECTORS_EXE --config "${CONFIG_FILE}" connector create --connector-service-config "$CONFIG_FILE" --update-config
        fi
    done
popd
