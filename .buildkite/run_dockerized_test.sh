#!/bin/bash
set -euo pipefail

docker run --rm -v $PWD:/ci -w=/ci \
    python:latest \
    /bin/bash -c  'make install PYTHON=python && make test'
