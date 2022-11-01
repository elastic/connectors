#!/bin/bash
set -euo pipefail

BASEDIR=$(realpath $(dirname $0))
ROOT=$(realpath $BASEDIR/../)

cd $ROOT/connectors/sources/tests/fixtures/mysql

make run-stack
sleep 120

make load-data

cd $ROOT
docker run --rm -v $ROOT:/ci -w=/ci \
    -it \
    python:3.10 \
    /bin/bash -c  "/ci/.buildkite/nightly.sh"

cd $ROOT/connectors/sources/tests/fixtures/mysql
make stop-stack

