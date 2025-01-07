#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

pyenv global $PYTHON_VERSION
echo "Python version:"
pyenv global

retry() {
  local retries=$1; shift
  local delay=$1; shift
  local attempts=1

  until "$@"; do
    retry_exit_status=$?
    echo "Exited with $retry_exit_status" >&2
    if (( retries == "0" )); then
      return $retry_exit_status
    elif (( attempts == retries )); then
      echo "Failed $attempts retries" >&2
      return $retry_exit_status
    else
      echo "Retrying $((retries - attempts)) more times..." >&2
      attempts=$((attempts + 1))
      sleep "$delay"
    fi
  done
}

is_pr_with_label() {
  match="$1"

  IFS=',' read -ra labels <<< "${GITHUB_PR_LABELS:-}"

  for label in "${labels[@]:-}"
  do
    if [ "$label" == "$match" ]; then
      return
    fi
  done

  false
}

is_auto_commit_disabled() {
  is_pr_with_label "ci:no-auto-commit"
}

make notice

if [ -z "$(git status --porcelain | grep NOTICE.txt)" ]; then
  exit 0
else 
  MACHINE_USERNAME="entsearchmachine"
  git config --global user.name "$MACHINE_USERNAME"
  git config --global user.email '90414788+entsearchmachine@users.noreply.github.com'
  GH_TOKEN="$VAULT_GITHUB_TOKEN"

  if ! is_auto_commit_disabled; then
    gh pr checkout "${BUILDKITE_PULL_REQUEST}"
    git add NOTICE.txt
    git commit -m"Update NOTICE.txt"
    git push
    sleep 15
  fi

  exit 1
fi

exit 0
