#!/bin/bash

set -o pipefail

if [[ ${CURDIR:-} == "" ]]; then
    export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
fi

source $CURDIR/set-env.sh

PYTHON_EXECUTABLE=""

if which python3 > /dev/null; then
    PY_VERSION=`python3 --version`
    PYTHON_EXECUTABLE="python3"
elif which python > /dev/null; then
    PY_VERSION=`python --version`
    PYTHON_EXECUTABLE="python"
fi

pushd $PROJECT_ROOT
    if [[ "${CONFIG_FILE:-}" == "" ]]; then
        CONFIG_FILE="${PROJECT_ROOT}/scripts/stack/connectors-config/config.yml"
    fi
    CLI_CONFIG="${PROJECT_ROOT}/scripts/stack/connectors-config/cli_config.yml"

    # ensure our Connectors CLI config exists and has the correct information
    if [ ! -f "$CLI_CONFIG" ]; then
        cliConfigText='
elasticsearch:
    host: http://localhost:9200
    password: '"${ELASTIC_PASSWORD}"'
    username: elastic
'
        echo "${cliConfigText}" > "$CLI_CONFIG"
    fi

    CONNECTORS_EXE="${PROJECT_ROOT}/bin/connectors"
    if [ ! -f "$CONNECTORS_EXE" ]; then
        echo "Could not find a connectors executable, running 'make clean install'"

        if [ $PYTHON_EXECUTABLE == "" ]; then
            echo "Could not find a suitable Python 3 executable..."
            exit 2
        fi

        make clean install PYTHON=$PYTHON_EXECUTABLE
    fi

    keep_configuring=true
    while [ $keep_configuring == true ]; do
        echo
        echo "Currently configured connectors:"
        $CONNECTORS_EXE --config "$CLI_CONFIG" connector list
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
            $CONNECTORS_EXE --config "${CLI_CONFIG}" connector create --connector-service-config "$CONFIG_FILE" --update-config
        fi
    done
popd
