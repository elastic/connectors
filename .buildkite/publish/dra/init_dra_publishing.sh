#!/bin/bash

########
# publishes a DRA artifact
########

set -exu
set -o pipefail

# Load our common environment variables for publishing
export CURDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source $CURDIR/../publish-common.sh

# Mock out creating an artifact
echo "Hello, world"
echo "The version is: ${VERSION}"