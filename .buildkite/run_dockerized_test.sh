#!/bin/bash

# TODO: convert all this install in a docker image we can just use
set -exuo pipefail

echo "Starting test task"
BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)
cd $ROOT

/usr/bin/python3.10 -m venv .
export PERF8_BIN=$ROOT/bin/perf8
export PYTHON=$ROOT/bin/python
export PIP=$ROOT/bin/pip
export DATA_SIZE=small
export VERSION=8.8.0-SNAPSHOT

$PIP install -r requirements/tests.txt
$PIP install -r requirements/x86_64.txt
$PYTHON setup.py develop

make test
make lint
make ftest NAME=dir DATA_SIZE=small PYTHON=$ROOT/bin/python
