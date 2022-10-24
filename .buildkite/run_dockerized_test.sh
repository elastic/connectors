#!/bin/bash
set -euo pipefail

docker run --rm -v $PWD:/ci -w=/ci \
    python:3.10 \
    /bin/bash -c  'make install PYTHON=python && make test'
