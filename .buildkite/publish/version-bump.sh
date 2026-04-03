#!/bin/bash

########
# Automated version bump for the DRA version bump pipeline.
#
# Expected environment variables (set by the centralized release-eng pipeline):
#   NEW_VERSION  - The target version (e.g. "9.5.0" for minor, "9.4.1" for patch)
#   BRANCH       - The release branch (e.g. "9.5" for minor, "9.4" for patch)
#   WORKFLOW     - "minor" or "patch"
########

set -euo pipefail

# Validate required environment variables
for var in NEW_VERSION BRANCH WORKFLOW; do
  if [[ -z "${!var:-}" ]]; then
    echo "Error: ${var} is not set"
    exit 1
  fi
done

if [[ "${WORKFLOW}" != "minor" && "${WORKFLOW}" != "patch" ]]; then
  echo "Error: WORKFLOW must be 'minor' or 'patch', got '${WORKFLOW}'"
  exit 1
fi

# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

VERSION_FILES=(
  "${PROJECT_ROOT}/app/connectors_service/connectors/VERSION"
  "${PROJECT_ROOT}/libs/connectors_sdk/connectors_sdk/VERSION"
)

PIPELINE_YML="${PROJECT_ROOT}/.buildkite/pipeline.yml"

# Configure git identity (not sourcing git-setup.sh because its
# `git switch -` fails in a fresh Buildkite checkout with no previous branch)
git config --local user.email 'elasticmachine@users.noreply.github.com'
git config --local user.name 'Elastic Machine'

# Bumps the version in all VERSION files, commits, and pushes to the given branch.
bump_version() {
  local version="$1"
  local branch="$2"

  echo "Bumping version to ${version} on branch ${branch}"

  for version_file in "${VERSION_FILES[@]}"; do
    echo "${version}" > "${version_file}"
    git add "${version_file}"
  done

  git commit -m "Bump version to ${version}"
  git push origin "${branch}"
}

if [[ "${WORKFLOW}" == "patch" ]]; then
  echo "=== Patch version bump: ${NEW_VERSION} on branch ${BRANCH} ==="
  git checkout "${BRANCH}"
  git pull origin "${BRANCH}"
  bump_version "${NEW_VERSION}" "${BRANCH}"

elif [[ "${WORKFLOW}" == "minor" ]]; then
  echo "=== Minor version bump: creating branch ${BRANCH} with version ${NEW_VERSION} ==="

  git checkout main
  git pull origin main

  # Add the new branch to pipeline.yml's DRA publishing condition so that
  # DRA artifacts are built on the new branch. The sed inserts the new branch
  # right after "main" in the hardcoded branch list.
  echo "Adding ${BRANCH} to DRA branch list in pipeline.yml"
  sed -i.bak "s/build.branch == \\\\\"main\\\\\"/build.branch == \\\\\"main\\\\\" || build.branch == \\\\\"${BRANCH}\\\\\"/" "${PIPELINE_YML}"
  rm -f "${PIPELINE_YML}.bak"
  git add "${PIPELINE_YML}"
  git commit -m "Add ${BRANCH} to DRA branch list"

  # Create the new release branch from main.
  # It inherits the pipeline.yml change and the current VERSION.
  git checkout -b "${BRANCH}"
  git push origin "${BRANCH}"

  # Bump main to the next minor version
  # e.g. if NEW_VERSION is 9.5.0, main should become 9.6.0
  IFS='.' read -ra VERSION_PARTS <<< "${NEW_VERSION}"
  NEXT_MINOR=$(( ${VERSION_PARTS[1]} + 1 ))
  NEXT_VERSION="${VERSION_PARTS[0]}.${NEXT_MINOR}.0"

  echo "=== Bumping main to next minor: ${NEXT_VERSION} ==="
  git checkout main
  bump_version "${NEXT_VERSION}" "main"
fi

echo "Version bump complete."
