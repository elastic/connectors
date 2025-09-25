#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

MACHINE_TYPE=`uname -m`

if [ "$MACHINE_TYPE" != "x86_64" ] && [ -v SKIP_AARCH64 ]; then
  echo "Running on aarch64 and skipping"
  exit
fi

source .buildkite/shared.sh

init_python

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)
VENV_ROOT=$ROOT/.venv

cd $ROOT

make install

export PIP=$VENV_ROOT/bin/pip

$PIP install py-spy


if [ -v BUILDKITE ]; then
  # required by serverless
  sudo sysctl -w vm.max_map_count=262144
fi

PERF8=yes NAME=$CONNECTOR DATA_SIZE=$DATA_SIZE make ftest
