#!/bin/bash
set -exuo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

# TODO to be moved in the image at https://github.com/elastic/ci-agent-images/blob/main/vm-images/enterprise-search/scripts/connectors-python/install-deps.sh#L6
sudo apt-get -y install liblz4-dev libunwind-dev

cd $ROOT

make install

export PIP=$ROOT/bin/pip

$PIP install py-spy
DATA_SIZE="${2:-small}"

PERF8=yes NAME=$1 DATA_SIZE=$DATA_SIZE make ftest
