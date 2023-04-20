#!/bin/bash
set -exuo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT

export PIP=$ROOT/bin/pip

$PIP install py-spy
connectors/tests/ftest.sh $1 yes
