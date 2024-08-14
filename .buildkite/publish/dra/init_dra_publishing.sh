#!/bin/bash

########
# publishes a DRA artifact
########

set -exu
set -o pipefail

# Load our common environment variables for publishing
export DRA_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CURDIR="$(dirname "$DRA_DIR")"
source $CURDIR/publish-common.sh
RELEASE_DIR="${PROJECT_ROOT}"

# Mock out creating an artifact
rm -rf $RELEASE_DIR/dist
mkdir -p $RELEASE_DIR/dist
cd $RELEASE_DIR/dist
zip test.zip -r . -i *
cd -

echo "The artifacts are: $(ls $RELEASE_DIR/dist)"


# ensure JQ is installed...
if ! which jq > /dev/null; then
  wget -O jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 && chmod +x jq
fi

# TODO keep copying from ent-search init_dra_publishing.sh