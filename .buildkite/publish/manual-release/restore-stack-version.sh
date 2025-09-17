#!/bin/bash
set -ex

# Load our common environment variables for publishing
export REL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CURDIR="$(dirname "$REL_DIR")"

source $CURDIR/publish-common.sh

echo $ORIG_VERSION > $PROJECT_ROOT/src/connectors/VERSION # removes the timestamp suffix
UPDATED_VERSION=`cat $PROJECT_ROOT/src/connectors/VERSION`

git add $PROJECT_ROOT/src/connectors/VERSION
git commit -m "Restoring version from ${VERSION} to ${UPDATED_VERSION}"
git push origin ${GIT_BRANCH}
