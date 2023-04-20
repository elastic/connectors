#!/bin/bash
set -exuo pipefail

# running the e2e test
connectors/tests/ftest.sh $1 yes
