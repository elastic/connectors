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

# Need to export the revision, name and version and others
# for the dependency report and publishing scripts
export REVISION="${BUILDKITE_COMMIT}"
export BRANCH_NAME="${BUILDKITE_BRANCH}"
export PRODUCT_NAME="enterprise-search"
export GIT_REPO="ent-search"

# set PUBLISH_SNAPSHOT and PUBLISH_STAGING based on the branch
if [[ "${BUILDKITE_BRANCH:-}" =~ (main|seanstory/add-DRA-support|[0-9]\.[0-9x]*$) ]]; then # TODO: remove my special branch
  export PUBLISH_SNAPSHOT="true"
fi
if [[ "${BUILDKITE_BRANCH:-}" =~ (seanstory/add-DRA-support|[0-9]\.[0-9x]*$) ]]; then # TODO: remove my special branch
  export PUBLISH_STAGING="true"
fi

# Sanity check in the logs to list the downloaded artifacts
chmod -R a+rw "${RELEASE_DIR}/dist"
echo "---- listing artifact directory: ${RELEASE_DIR}/dist:"
ls -al $RELEASE_DIR/dist

# function to set our vault creds for DRA publishing
function setDraVaultCredentials() {
  set +x
  echo "-- setting DRA vault credentials --"

  # need to keep original vault addr to reset it
  # after we're done or else it won't be set
  # if we call this fxn again
  export ORIGINAL_VAULT_ADDR="${VAULT_ADDR:-}"

  DRA_CREDS=$(vault kv get -field=data -format=json kv/ci-shared/release/dra-role)
  if [[ "${DRA_CREDS:-}" == "" ]]; then
    echo "Could not get DRA credentials from vault"
    exit 2
  fi

  export VAULT_ADDR="$(echo $DRA_CREDS | jq -r '.vault_addr')"
  if [[ "${VAULT_ADDR:-}" == "" ]]; then
    echo "Could not get VAULT_ADDR from DRA credentials"
    exit 2
  fi

  export VAULT_ROLE_ID="$(echo $DRA_CREDS | jq -r '.role_id')"
  if [[ "${VAULT_ROLE_ID:-}" == "" ]]; then
    echo "Could not get VAULT_ADDR from DRA credentials"
    exit 2
  fi

  export VAULT_SECRET_ID="$(echo $DRA_CREDS | jq -r '.secret_id')"
  if [[ "${VAULT_SECRET_ID:-}" == "" ]]; then
    echo "Could not get VAULT_ADDR from DRA credentials"
    exit 2
  fi

  unset DRA_CREDS
  set -x
}

# function to unset the vault credentials.
function unsetDraVaultCredentials() {
  set +x
  unset VAULT_ROLE_ID
  unset VAULT_SECRET_ID

  # and reset our original vault address
  export VAULT_ADDR="${ORIGINAL_VAULT_ADDR:-}"

  set -x
}

# publish SNAPSHOT artifacts
if [[ "${PUBLISH_SNAPSHOT:-}" == "true" ]]; then

  echo "-------- Publishing SNAPSHOT DRA Artifacts"
  setDraVaultCredentials
  export WORKFLOW="snapshot"

  # TODO - when ready, remove the gate and ungate test vars
  if [[ "${DRA_PUBLISH_GATE:-}" == "" || "${DRA_UNGATE_TEST:-}" == "true" ]]; then
    source "${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh"
  else
    echo "DRA_PUBLISH_GATE is locked. This is where we would call 'source ${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh'"
  fi
  unsetDraVaultCredentials
fi

# publish STAGING artifacts
if [[ "${PUBLISH_STAGING:-}" == "true" ]]; then

  echo "-------- Publishing STAGING DRA Artifacts"
  setDraVaultCredentials
  export WORKFLOW="staging"

  # TODO - when ready, remove the gate var
  if [[ "${DRA_PUBLISH_GATE:-}" == "" ]]; then
    source "${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh"
  else
    echo "DRA_PUBLISH_GATE is locked. This is where we would call 'source ${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh'"
  fi
  unsetDraVaultCredentials
  rm -rf "${DEPENDENCIES_REPORTS_DIR}/*"
fi
