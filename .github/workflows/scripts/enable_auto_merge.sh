#!/usr/bin/env bash
# Enable squash auto-merge on a same-repo backport PR targeting a maintenance
# branch (X.Y). Auto-merge only completes once the ruleset is satisfied
# (required review, buildkite/connectors, CLA), so enabling it early is safe.
#
# Usage: enable_auto_merge.sh <pr-number>
# Env:   GH_REPO        owner/name
#        DEFAULT_TOKEN  GITHUB_TOKEN (tried first)
#        BOT_TOKEN      REPO_SCOPED_TOKEN (fallback; the bot PAT alone is
#                       rejected on protected branches)
#
# Never fails the workflow: if auto-merge cannot be enabled it only warns and
# the PR has to be merged manually once the checks pass.
set -uo pipefail

PR_NUMBER="${1:?usage: enable_auto_merge.sh <pr-number>}"

first_token="${DEFAULT_TOKEN:-${BOT_TOKEN:-}}"
info=$(GH_TOKEN="${first_token}" gh pr view "${PR_NUMBER}" \
  --json baseRefName,isCrossRepository,labels,autoMergeRequest \
  --jq '[.baseRefName, (.isCrossRepository|tostring), ([.labels[].name]|index("backport")|tostring), (.autoMergeRequest != null | tostring)] | @tsv') || {
  echo "::warning::Could not read PR #${PR_NUMBER}; skipping auto-merge."
  exit 0
}
IFS=$'\t' read -r base cross label enabled <<<"${info}"

if ! [[ "${base}" =~ ^[0-9]+\.[0-9]+$ ]]; then
  echo "PR #${PR_NUMBER}: base '${base}' is not a maintenance branch; skipping."
  exit 0
fi
if [ "${cross}" != "false" ]; then
  echo "PR #${PR_NUMBER}: cross-repository PR; skipping."
  exit 0
fi
if [ "${label}" = "null" ]; then
  echo "PR #${PR_NUMBER}: no 'backport' label; skipping."
  exit 0
fi
if [ "${enabled}" = "true" ]; then
  echo "PR #${PR_NUMBER}: auto-merge already enabled."
  exit 0
fi

for token in "${DEFAULT_TOKEN:-}" "${BOT_TOKEN:-}"; do
  [ -n "${token}" ] || continue
  # Plain `--auto` covers branches where a merge queue dictates the method and
  # an explicit one is rejected.
  for method in "--squash" ""; do
    # shellcheck disable=SC2086
    if out=$(GH_TOKEN="${token}" gh pr merge "${PR_NUMBER}" --auto ${method} 2>&1); then
      echo "PR #${PR_NUMBER}: auto-merge enabled (${method:-default method})."
      exit 0
    fi
    if grep -qi "already" <<<"${out}"; then
      echo "PR #${PR_NUMBER}: auto-merge already enabled."
      exit 0
    fi
    echo "Attempt failed: ${out}"
  done
done

echo "::warning::Could not enable auto-merge on PR #${PR_NUMBER}; merge it manually once checks pass."
exit 0
