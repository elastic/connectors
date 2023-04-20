#!/bin/bash

# TODO: convert all this install in a docker image we can just use
set -exuo pipefail

make test
make lint
make ftest NAME=dir DATA_SIZE=small PYTHON=$ROOT/bin/python
