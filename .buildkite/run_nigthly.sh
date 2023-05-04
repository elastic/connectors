#!/bin/bash
set -exuo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

make install

export PIP=$ROOT/bin/pip

$PIP install py-spy
PERF8=yes NAME=$1 DATA_SIZE=small make ftest
