#!/bin/bash
set -euo pipefail

docker run --rm --privileged -v $PWD:/ci -w=/ci \
     -v /var/run/docker.sock:/var/run/docker.sock \
    -it \
    python:latest \
    /bin/bash -c  "/ci/.buildkite/nightly.sh"
