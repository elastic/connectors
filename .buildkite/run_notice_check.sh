#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh
source .buildkite/publish/git-setup.sh

init_python

make notice

if [ -z "$(git status --porcelain | grep NOTICE.txt)" ]; then
  exit 0
else 
  git --no-pager diff
  if is_pr; then
    export GH_TOKEN="$VAULT_GITHUB_TOKEN"

    git add NOTICE.txt
    git commit -m"Update NOTICE.txt"
    git push
    sleep 15
  fi

  exit 1
fi

exit 0
