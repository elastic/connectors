#!/bin/bash

# TODO: convert all this install in a docker image we can just use
set -exuo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

/usr/bin/python3.10 -m venv .
export PYTHON=$ROOT/bin/python
export PIP=$ROOT/bin/pip
export DATA_SIZE=small
export VERSION=8.8.0-SNAPSHOT

make ftest NAME=dir DATA_SIZE=small PYTHON=$ROOT/bin/python
