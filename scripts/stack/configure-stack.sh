#!/bin/sh

set -xo pipefail

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

# set up and activate our virtual env
keep_venv="true"
if [ ! -d ./venv ]; then
    $PYTHON_EXECUTABLE -m venv venv
    keep_venv="false"
fi

source ./venv/bin/activate
$PYTHON_EXECUTABLE -m ensurepip --default-pip
$PYTHON_EXECUTABLE -m pip install -r requirements.txt

$PYTHON_EXECUTABLE configure_stack.py "$OUTPUT_DIR/created_config.yml"

deactivate
if [ $keep_venv == "false" ]; then
    rm -r ./venv
fi
