#!/bin/bash
set -ex

# Load our common environment variables for publishing
export REL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
CURDIR="$(dirname "$REL_DIR")"

source $CURDIR/publish-common.sh

echo $VERSION > $PROJECT_ROOT/src/connectors/VERSION # adds the timestamp suffix
UPDATED_VERSION=`cat $PROJECT_ROOT/src/connectors/VERSION`

git add $PROJECT_ROOT/src/connectors/VERSION
git commit -m "Bumping version from ${ORIG_VERSION} to ${UPDATED_VERSION}"
git push origin ${GIT_BRANCH}

echo "Tagging the release"
git tag "v${UPDATED_VERSION}"
git push origin --tags
