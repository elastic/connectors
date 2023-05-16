#!/bin/bash
set -exuo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

make install

export PIP=$ROOT/bin/pip

$PIP install py-spy
DATA_SIZE="${2:-small}"

PERF8=yes NAME=$1 DATA_SIZE=$DATA_SIZE make ftest
