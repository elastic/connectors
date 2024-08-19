#!/bin/bash

set -exu
set -o pipefail

#---------------------------------------------------------------------------------------------------

# Ensure all required VAULT variables are set.
# and ensure we're not leaking credentials...
# These are hidden in Jenkins, but not necessarily in Buildkite
set +x
if [[ "${VAULT_ADDR:-}" == "" ]]; then
  echo "ERROR: VAULT_ADDR required!"
  exit 2
else
  export "VAULT_ADDR=${VAULT_ADDR}"
fi

if [[ "${VAULT_ROLE_ID:-}" == "" ]]; then
  echo "ERROR: VAULT_ROLE_ID required!"
  exit 2
else
  export "VAULT_ROLE_ID=${VAULT_ROLE_ID}"
fi

if [[ "${VAULT_SECRET_ID:-}" == "" ]]; then
  echo "ERROR: VAULT_SECRET_ID required!"
  exit 2
else
  export "VAULT_SECRET_ID=${VAULT_SECRET_ID}"
fi
set -x

# Release dir required. If this is not an absolute path the Docker command will error.
if [[ "${RELEASE_DIR:-}" == "" ]]; then
  echo "ERROR: RELEASE_DIR required!"
  exit 2
fi

# Git Repo
if [[ "${GIT_REPO:-}" == "" ]]; then
  echo "ERROR: GIT_REPO required!"
  exit 2
fi

# Revision Git commit hash
if [[ "${REVISION:-}" == "" ]]; then
  echo "ERROR: REVISION required!"
  exit 2
fi

# Workflow. Must be either `staging` or `snapshot`
if [[ "${WORKFLOW:-}" != "staging" && "${WORKFLOW:-}" != "snapshot" ]]; then
  echo "ERROR: WORKFLOW required! Value must be staging or snapshot"
  exit 2
fi

# Version. This is pulled from config/product_version.
if [[ "${VERSION:-}" == "" ]]; then
  echo "ERROR: VERSION required!"
  exit 2
fi

# Branch Name. Required for the cli collect artifact publish
if [[ "${BRANCH_NAME:-}" == "" ]]; then
  echo "ERROR: BRANCH_NAME required!"
  exit 2
fi

#---------------------------------------------------------------------------------------------------
echo "Made it to 'docker run'" # TODO, replace this with actual docker run
echo "Conents of 'src':"
ls -lah ${RELEASE_DIR}/dist
BRANCH_NAME="main" #TODO, hacking around dev branch iterations
docker run --rm \
  --name release-manager \
  -e VAULT_ADDR \
  -e VAULT_ROLE_ID \
  -e VAULT_SECRET_ID \
  --mount type=bind,readonly=false,src="${RELEASE_DIR}/dist",target="/artifacts" \
  docker.elastic.co/infra/release-manager:latest \
  cli collect \
      --project "${GIT_REPO}" \
      --branch "${BRANCH_NAME}" \
      --commit "${REVISION}" \
      --workflow "${WORKFLOW}" \
      --version "${VERSION}" \
      --artifact-set main