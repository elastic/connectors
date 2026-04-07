#!/bin/bash

########
# Automated version bump for the DRA version bump pipeline.
#
# Expected environment variables (set by the centralized release-eng pipeline):
#   NEW_VERSION  - The target version (e.g. "9.5.0" for minor, "9.4.1" for patch)
#   BRANCH       - The release branch (e.g. "9.5" for minor, "9.4" for patch)
#   WORKFLOW     - "minor" or "patch"
#
# Optional:
#   DRY_RUN      - Set to "true" to print what would happen without pushing or committing
########

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"

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

git config --local user.email 'elasticmachine@users.noreply.github.com'
git config --local user.name 'Elastic Machine'

# Bumps the version in all VERSION files, commits, and pushes to the given branch.
bump_version() {
  local version="$1"
  local branch="$2"

  echo "Bumping version to ${version} on branch ${branch}"

  for version_file in "${VERSION_FILES[@]}"; do
    if [[ "${DRY_RUN}" == "true" ]]; then
      echo "[dry-run] Would write '${version}' to ${version_file}"
    else
      echo "${version}" > "${version_file}"
      git add "${version_file}"
    fi
  done

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would commit: 'Bump version to ${version}'"
    echo "[dry-run] Would push to origin/${branch}"
  else
    git commit -m "Bump version to ${version}"
    push_with_retry "${branch}"
  fi
}

# Adds a branch to the DRA branch list in pipeline.yml.
# The list is the `if:` condition on the "Packaging and DRA" group step.
# Uses Python to safely parse and update the condition rather than fragile sed.
add_branch_to_dra_list() {
  local new_branch="$1"

  echo "Adding ${new_branch} to DRA branch list in pipeline.yml"

  python3 -c '
import sys

pipeline_yml = sys.argv[1]
new_branch = sys.argv[2]

pipeline = open(pipeline_yml).read()

# Find the DRA group if-condition line using the ci:packaging sentinel.
dra_marker = "ci:packaging"
lines = pipeline.splitlines(True)
target_idx = None
for i, line in enumerate(lines):
    if dra_marker in line and line.strip().startswith("if:"):
        target_idx = i
        break

if target_idx is None:
    print("Error: could not find DRA branch condition in pipeline.yml", file=sys.stderr)
    sys.exit(1)

line = lines[target_idx]

# Check if branch is already listed (quotes are escaped as \" in YAML)
escaped_branch = f"\\\"" + new_branch + f"\\\""
if escaped_branch in line:
    print(f"Branch {new_branch} already in DRA list, skipping")
    sys.exit(0)

# Insert new branch condition before the pull_request.labels clause
pr_label_marker = "build.pull_request.labels"
if pr_label_marker not in line:
    print("Error: could not find pull_request.labels sentinel in condition", file=sys.stderr)
    sys.exit(1)

insertion = f"build.branch == \\\"{new_branch}\\\" || "
lines[target_idx] = line.replace(pr_label_marker, insertion + pr_label_marker)

open(pipeline_yml, "w").write("".join(lines))
print("Updated DRA branch list successfully")
' "${PIPELINE_YML}" "${new_branch}"
}

MAX_PUSH_RETRIES=3

# Push with retry to handle concurrent commits landing on the target branch.
push_with_retry() {
  local branch="$1"
  local attempt

  for attempt in $(seq 1 "${MAX_PUSH_RETRIES}"); do
    if git push origin "${branch}"; then
      return 0
    fi
    echo "Push to ${branch} failed (attempt ${attempt}/${MAX_PUSH_RETRIES}), rebasing and retrying..."
    git pull --rebase origin "${branch}"
  done

  echo "Error: failed to push to ${branch} after ${MAX_PUSH_RETRIES} attempts"
  return 1
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

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would add ${BRANCH} to DRA branch list in pipeline.yml"
    echo "[dry-run] Would commit: 'Add ${BRANCH} to DRA branch list'"
  else
    add_branch_to_dra_list "${BRANCH}"
    git add "${PIPELINE_YML}"
    git commit -m "Add ${BRANCH} to DRA branch list"
  fi

  # Create the new release branch from main.
  # It inherits the pipeline.yml change and the current VERSION.
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create and push branch ${BRANCH}"
  else
    git checkout -b "${BRANCH}"
    git push origin "${BRANCH}"
  fi

  # Bump main to the next minor version
  # e.g. if NEW_VERSION is 9.5.0, main should become 9.6.0
  IFS='.' read -ra VERSION_PARTS <<< "${NEW_VERSION}"
  NEXT_MINOR=$(( ${VERSION_PARTS[1]} + 1 ))
  NEXT_VERSION="${VERSION_PARTS[0]}.${NEXT_MINOR}.0"

  echo "=== Bumping main to next minor: ${NEXT_VERSION} ==="
  if [[ "${DRY_RUN}" != "true" ]]; then
    git checkout main
  fi
  bump_version "${NEXT_VERSION}" "main"
fi

echo "Version bump complete."
