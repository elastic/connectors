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
#   AUTO_MERGE   - Set to "false" to create PRs but skip enabling auto-merge and the
#                  merge-wait loop. Useful for testing: PRs stay open for inspection
#                  and closure. Defaults to "true".
########

set -euo pipefail

DRY_RUN="${DRY_RUN:-false}"
AUTO_MERGE="${AUTO_MERGE:-true}"

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

PR_MERGE_TIMEOUT=5400  # 90 minutes -- accommodates full PR CI (~30 min) plus retries/flakes
PR_POLL_INTERVAL=30    # seconds

# Reads the current version string from the first VERSION file.
current_version() {
  cat "${VERSION_FILES[0]}" 2>/dev/null || echo ""
}

# Checks if the first VERSION file already contains the given version.
version_already_bumped() {
  [[ "$(current_version)" == "$1" ]]
}

# Returns 0 if bumping to the given target would be a downgrade
# (i.e. the current version is strictly greater than the target).
# Uses `sort -V` for semver-aware comparison.
version_is_downgrade() {
  local target="$1"
  local current
  current="$(current_version)"
  [[ -z "${current}" ]] && return 1
  [[ "${current}" == "${target}" ]] && return 1
  # sort -V -C exits 0 iff input is already sorted (current <= target).
  # Negate: true iff current > target.
  ! printf '%s\n%s\n' "${current}" "${target}" | sort -V -C 2>/dev/null
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
# Delegates to add_branch_to_dra_list.py so the logic can be unit tested.
add_branch_to_dra_list() {
  local new_branch="$1"
  echo "Adding ${new_branch} to DRA branch list in pipeline.yml"
  python3 "${SCRIPT_DIR}/add_branch_to_dra_list.py" "${PIPELINE_YML}" "${new_branch}"
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

  # Reuse an existing open PR if there is one. `--state open` is required:
  # branch names get recycled across runs, so we'd otherwise match a stale
  # merged PR and then fail trying to auto-merge it.
  local pr_url
  pr_url=$(gh pr list --head "${source_branch}" --state open --json url --jq '.[0].url // empty' 2>/dev/null || echo "")

  if [[ -n "${pr_url}" ]]; then
    echo "PR already exists: ${pr_url}"
  else
    pr_url=$(gh pr create \
      --title "${title}" \
      --body "Automated version bump PR created by the [version bump pipeline](${BUILDKITE_BUILD_URL:-})." \
      --base "${target_branch}" \
      --head "${source_branch}")
    echo "Created PR: ${pr_url}"
  fi

  if [[ "${AUTO_MERGE}" != "true" ]]; then
    echo "AUTO_MERGE=false -- leaving PR open without enabling auto-merge: ${pr_url}"
    return 0
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

  if version_is_downgrade "${NEW_VERSION}"; then
    echo "Error: ${BRANCH} is already at $(current_version), refusing to downgrade to ${NEW_VERSION}"
    exit 1
  elif version_already_bumped "${NEW_VERSION}"; then
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

  if version_is_downgrade "${NEXT_VERSION}"; then
    echo "Error: main is already at $(current_version), refusing to downgrade to ${NEXT_VERSION}"
    exit 1
  elif version_already_bumped "${NEXT_VERSION}"; then
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
