#!/bin/bash

set -o pipefail

OUTPUT_DIR=${1:-}
if [[ ${OUTPUT_DIR:-} == "" ]]; then
    echo "Missing argument 1 for OUTPUT_DIR"
    exit 2
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
    CONNECTORS_EXE="${PROJECT_ROOT}/bin/connectors"
    if [ ! -f "$CONNECTORS_EXE" ]; then
        echo "Could not find a connectors executable, running 'make clean install'"
        make clean install PYTHON=$PYTHON_EXECUTABLE
    fi

    keep_configuring=true
    while [ $keep_configuring == true ]; do
        echo
        echo "Currently configured connectors:"
        $CONNECTORS_EXE --config scripts/stack/connectors-config/config.yml connector list
        echo
        echo "Do you want to set up a new connector?"
        select ync in "Yes" "No" "Cancel"; do
            case $ync in
            Yes ) break;;
            No ) keep_configuring=false; break;;
            Cancel ) popd; exit 1;;
            esac
        done
        if [ $keep_configuring == true ]; then
            $CONNECTORS_EXE --config scripts/stack/connectors-config/config.yml connector create
        fi
    done
popd
