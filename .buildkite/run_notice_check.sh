#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python

make notice

if [ -z "$(git status --porcelain | grep NOTICE.txt)" ]; then
  exit 0
else 
  MACHINE_USERNAME="entsearchmachine"
  git config --global user.name "$MACHINE_USERNAME"
  git config --global user.email '90414788+entsearchmachine@users.noreply.github.com'
  export GH_TOKEN="$VAULT_GITHUB_TOKEN"

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
