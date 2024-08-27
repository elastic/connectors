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
export RELEASE_DIR="${PROJECT_ROOT}"

# Create and stage the artifact
cd $PROJECT_ROOT
make clean sdist
export DRA_ARTIFACTS_DIR=$RELEASE_DIR/dist/dra-artifacts
mkdir -p $DRA_ARTIFACTS_DIR

cd -

# Download previous step artifacts
buildkite-agent artifact download '.artifacts/*.tar.gz*' $RELEASE_DIR/dist/ --step build_docker_image_amd64
buildkite-agent artifact download '.artifacts/*.tar.gz*' $RELEASE_DIR/dist/ --step build_docker_image_arm64
cp $RELEASE_DIR/dist/.artifacts/* $DRA_ARTIFACTS_DIR

# Rename to match DRA expectations (<name>-<version>-<classifier>-<os>-<arch>)
cd $DRA_ARTIFACTS_DIR
mv $DOCKER_ARTIFACT_KEY-$VERSION-amd64.tar.gz $PROJECT_NAME-$VERSION-docker-image-linux-amd64.tar.gz
mv $DOCKER_ARTIFACT_KEY-$VERSION-arm64.tar.gz $PROJECT_NAME-$VERSION-docker-image-linux-arm64.tar.gz
cd -

echo "The artifacts are: $(ls $DRA_ARTIFACTS_DIR)"


# ensure JQ is installed...
if ! which jq > /dev/null; then
  echo "'jq' was not installed. Installing it now"
  wget -O jq https://github.com/stedolan/jq/releases/download/jq-1.6/jq-linux64 && chmod +x jq
else
  echo "jq already installed"
fi

# Need to export the revision, name and version and others
# for the dependency report and publishing scripts
export REVISION="${BUILDKITE_COMMIT}"
export BRANCH_NAME="${BUILDKITE_BRANCH}"
export PRODUCT_NAME="connectors"
export GIT_REPO="connectors"

# set PUBLISH_SNAPSHOT and PUBLISH_STAGING based on the branch
if [[ "${BUILDKITE_BRANCH:-}" =~ (main|[0-9]\.[0-9x]*$) ]]; then
  export PUBLISH_SNAPSHOT="true"
fi
if [[ "${BUILDKITE_BRANCH:-}" =~ ([0-9]\.[0-9x]*$) ]]; then
  export PUBLISH_STAGING="true"
fi

# Sanity check in the logs to list the downloaded artifacts
chmod -R a+rw "${RELEASE_DIR}/dist"
echo "---- listing artifact directory: ${RELEASE_DIR}/dist:"
ls -al $RELEASE_DIR/dist

# ensure we have a directory and permissions for our dependency report to be created
# this path is set in the actual config file in
# https://github.com/elastic/infra/tree/master/cd/release/release-manager/project-configs
export DEPENDENCIES_REPORTS_DIR="${RELEASE_DIR}/dist/cd/output" # TODO, change this path?
mkdir -p "${DEPENDENCIES_REPORTS_DIR}"
chmod -R a+rw "${DEPENDENCIES_REPORTS_DIR}"

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

# function to generate dependency report.
function generateDependencyReport() {
  make deps-csv
  cp $RELEASE_DIR/dist/dependencies.csv $1
}

# generate the dependency report and publish SNAPSHOT artifacts
if [[ "${PUBLISH_SNAPSHOT:-}" == "true" ]]; then
  # note that this name is set by infra and must be in the format specified
  dependencyReportName="dependencies-${VERSION}-SNAPSHOT.csv";
  echo "-------- Generating SNAPSHOT dependency report: ${dependencyReportName}"
  generateDependencyReport $DEPENDENCIES_REPORTS_DIR/$dependencyReportName

  echo "-------- Publishing SNAPSHOT DRA Artifacts"
  cp $RELEASE_DIR/dist/elasticsearch_connectors-${VERSION}.zip $DRA_ARTIFACTS_DIR/connectors-${VERSION}-SNAPSHOT.zip
  cp $DRA_ARTIFACTS_DIR/$PROJECT_NAME-$VERSION-docker-image-linux-amd64.tar.gz $DRA_ARTIFACTS_DIR/$PROJECT_NAME-$VERSION-SNAPSHOT-docker-image-linux-amd64.tar.gz
  cp $DRA_ARTIFACTS_DIR/$PROJECT_NAME-$VERSION-docker-image-linux-arm64.tar.gz $DRA_ARTIFACTS_DIR/$PROJECT_NAME-$VERSION-SNAPSHOT-docker-image-linux-arm64.tar.gz
  setDraVaultCredentials
  export WORKFLOW="snapshot"

  source "${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh"
  unsetDraVaultCredentials
  rm -rf "${DEPENDENCIES_REPORTS_DIR}/*"
else
  echo "Not publishing SNAPSHOTs in this build"
fi

# generate the dependency report and publish STAGING artifacts
if [[ "${PUBLISH_STAGING:-}" == "true" ]]; then
  dependencyReportName="dependencies-${VERSION}.csv";
  echo "-------- Generating STAGING dependency report: ${dependencyReportName}"
  generateDependencyReport $DEPENDENCIES_REPORTS_DIR/$dependencyReportName

  echo "-------- Publishing STAGING DRA Artifacts"
  cp $RELEASE_DIR/dist/elasticsearch_connectors-${VERSION}.zip $DRA_ARTIFACTS_DIR/connectors-${VERSION}.zip
  setDraVaultCredentials
  export WORKFLOW="staging"


  source "${PROJECT_ROOT}/.buildkite/publish/dra/publish-daily-release-artifact.sh"
  unsetDraVaultCredentials
  rm -rf "${DEPENDENCIES_REPORTS_DIR}/*"
else
  echo "Not publishing to staging in this build"
fi
