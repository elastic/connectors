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

# The Buildkite environment hook provides VAULT_GITHUB_TOKEN (ephemeral PAT
# generated from the GithubPermissionSet in elastic/terrazzo). gh CLI needs
# it exported as GH_TOKEN.
if [[ "${DRY_RUN}" != "true" ]]; then
  export GH_TOKEN="${VAULT_GITHUB_TOKEN:?VAULT_GITHUB_TOKEN is not set}"
fi

PR_MERGE_TIMEOUT=3600  # 60 minutes
PR_POLL_INTERVAL=30    # seconds

# Checks if the first VERSION file already contains the given version.
version_already_bumped() {
  local version="$1"
  local current
  current=$(cat "${VERSION_FILES[0]}" 2>/dev/null || echo "")
  [[ "${current}" == "${version}" ]]
}

# Checks if a remote branch exists.
remote_branch_exists() {
  local branch="$1"
  git ls-remote --exit-code --heads origin "${branch}" >/dev/null 2>&1
}

# Writes the given version to all VERSION files and stages them.
write_version_files() {
  local version="$1"
  for version_file in "${VERSION_FILES[@]}"; do
    echo "${version}" > "${version_file}"
    git add "${version_file}"
  done
}

# Adds a branch to the DRA branch list in pipeline.yml.
# The list is the `if:` condition on the "Packaging and DRA" group step.
add_branch_to_dra_list() {
  local new_branch="$1"

  echo "Adding ${new_branch} to DRA branch list in pipeline.yml"

  python3 -c '
import sys

pipeline_yml = sys.argv[1]
new_branch = sys.argv[2]

with open(pipeline_yml) as f:
    pipeline = f.read()

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
escaped = "\\\""
if f"{escaped}{new_branch}{escaped}" in line:
    print(f"Branch {new_branch} already in DRA list, skipping")
    sys.exit(0)

# Insert new branch condition before the pull_request.labels clause
pr_label_marker = "build.pull_request.labels"
if pr_label_marker not in line:
    print("Error: could not find pull_request.labels marker in condition", file=sys.stderr)
    sys.exit(1)

insertion = f"build.branch == {escaped}{new_branch}{escaped} || "
lines[target_idx] = line.replace(pr_label_marker, insertion + pr_label_marker)

with open(pipeline_yml, "w") as f:
    f.write("".join(lines))
print("Updated DRA branch list successfully")
' "${PIPELINE_YML}" "${new_branch}"
}

# Creates a PR with auto-merge enabled and waits for it to merge.
create_pr_and_wait() {
  local title="$1"
  local target_branch="$2"
  local source_branch="$3"

  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create PR: '${title}' (${source_branch} -> ${target_branch})"
    echo "[dry-run] Would enable auto-merge and wait for merge"
    return 0
  fi

  echo "Creating PR: ${title} (${source_branch} -> ${target_branch})"

  # Check if a PR already exists for this branch (idempotency on re-run)
  local pr_url
  pr_url=$(gh pr view "${source_branch}" --json url --jq '.url' 2>/dev/null || echo "")

  if [[ -n "${pr_url}" ]]; then
    echo "PR already exists: ${pr_url}"
  else
    pr_url=$(gh pr create \
      --title "${title}" \
      --body "Automated version bump PR created by the [version bump pipeline](${BUILDKITE_BUILD_URL:-})." \
      --base "${target_branch}" \
      --head "${source_branch}" \
      --label "ci:version-bump")
    echo "Created PR: ${pr_url}"
  fi

  gh pr merge "${pr_url}" --auto --squash --delete-branch
  echo "Auto-merge enabled for ${pr_url}"

  # Poll until the PR is merged or closed
  local elapsed=0
  while [[ $elapsed -lt $PR_MERGE_TIMEOUT ]]; do
    local state
    state=$(gh pr view "${pr_url}" --json state --jq '.state')
    if [[ "${state}" == "MERGED" ]]; then
      echo "PR merged: ${pr_url}"
      return 0
    elif [[ "${state}" == "CLOSED" ]]; then
      echo "Error: PR was closed without merging: ${pr_url}"
      return 1
    fi
    echo "Waiting for PR to merge (${elapsed}s elapsed)..."
    sleep "${PR_POLL_INTERVAL}"
    elapsed=$((elapsed + PR_POLL_INTERVAL))
  done

  echo "Error: timed out waiting for PR to merge after ${PR_MERGE_TIMEOUT}s: ${pr_url}"
  return 1
}

if [[ "${WORKFLOW}" == "patch" ]]; then
  echo "=== Patch version bump: ${NEW_VERSION} on branch ${BRANCH} ==="

  git checkout "${BRANCH}"
  git pull origin "${BRANCH}"

  pr_branch="automations/bump-${BRANCH}-to-${NEW_VERSION}"

  if version_already_bumped "${NEW_VERSION}"; then
    echo "Version already at ${NEW_VERSION} on ${BRANCH}, nothing to do"
  elif [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create branch ${pr_branch}"
    echo "[dry-run] Would bump VERSION files to ${NEW_VERSION}"
    echo "[dry-run] Would create PR targeting ${BRANCH} and wait for merge"
  else
    if ! remote_branch_exists "${pr_branch}"; then
      git checkout -b "${pr_branch}"
      write_version_files "${NEW_VERSION}"
      git commit -m "Bump version to ${NEW_VERSION}"
      git push origin "${pr_branch}"
    fi

    create_pr_and_wait \
      "Bump version to ${NEW_VERSION}" \
      "${BRANCH}" \
      "${pr_branch}"
  fi

elif [[ "${WORKFLOW}" == "minor" ]]; then
  echo "=== Minor version bump: creating branch ${BRANCH} with version ${NEW_VERSION} ==="

  git checkout main
  git pull origin main

  # Step 1: PR to add the new branch to pipeline.yml's DRA condition.
  # This must merge before we create the release branch so the branch
  # inherits the updated pipeline.yml.
  #
  # Check if the branch is already in the DRA list on main (i.e. the PR
  # already merged in a previous run).
  pr_branch="automations/add-${BRANCH}-to-dra-list"

  if grep -q "build.branch == \\\\\"${BRANCH}\\\\\"" "${PIPELINE_YML}"; then
    echo "Branch ${BRANCH} already in DRA list on main, skipping step 1"
  elif [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create branch ${pr_branch}"
    echo "[dry-run] Would add ${BRANCH} to DRA branch list in pipeline.yml"
    echo "[dry-run] Would create PR targeting main and wait for merge"
  else
    if ! remote_branch_exists "${pr_branch}"; then
      git checkout -b "${pr_branch}"
      add_branch_to_dra_list "${BRANCH}"
      git add "${PIPELINE_YML}"
      git commit -m "Add ${BRANCH} to DRA branch list"
      git push origin "${pr_branch}"
    fi

    create_pr_and_wait \
      "Add ${BRANCH} to DRA branch list" \
      "main" \
      "${pr_branch}"
  fi

  # Step 2: Create the release branch from the updated main.
  # The branch inherits the pipeline.yml change and the current VERSION.
  if remote_branch_exists "${BRANCH}"; then
    echo "Release branch ${BRANCH} already exists, skipping creation"
  elif [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create and push release branch ${BRANCH} from main"
  else
    git checkout main
    git pull origin main
    git checkout -b "${BRANCH}"
    git push origin "${BRANCH}"
    echo "Created release branch: ${BRANCH}"
  fi

  # Step 3: PR to bump main to the next minor version.
  IFS='.' read -ra VERSION_PARTS <<< "${NEW_VERSION}"
  NEXT_MINOR=$(( ${VERSION_PARTS[1]} + 1 ))
  NEXT_VERSION="${VERSION_PARTS[0]}.${NEXT_MINOR}.0"

  echo "=== Bumping main to next minor: ${NEXT_VERSION} ==="

  pr_branch="automations/bump-main-to-${NEXT_VERSION}"

  git checkout main
  git pull origin main

  if version_already_bumped "${NEXT_VERSION}"; then
    echo "Main already at ${NEXT_VERSION}, nothing to do"
  elif [[ "${DRY_RUN}" == "true" ]]; then
    echo "[dry-run] Would create branch ${pr_branch}"
    echo "[dry-run] Would bump VERSION files to ${NEXT_VERSION}"
    echo "[dry-run] Would create PR targeting main and wait for merge"
  else
    if ! remote_branch_exists "${pr_branch}"; then
      git checkout -b "${pr_branch}"
      write_version_files "${NEXT_VERSION}"
      git commit -m "Bump version to ${NEXT_VERSION}"
      git push origin "${pr_branch}"
    fi

    create_pr_and_wait \
      "Bump version to ${NEXT_VERSION}" \
      "main" \
      "${pr_branch}"
  fi
fi

echo "Version bump complete."
