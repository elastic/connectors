#!/bin/bash
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
$PIP install py-spy

# running the e2e test
connectors/tests/ftest.sh $1 yes
