#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh


init_python

if is_pr && ! is_fork; then
  echo 'Running on a PR that is not a fork, will commit changes'

  export GH_TOKEN="$VAULT_GITHUB_TOKEN"
  source .buildkite/publish/git-setup.sh
  make notice

  if [ -z "$(git status --porcelain | grep NOTICE.txt)" ]; then
    echo 'Nothing changed'
    exit 0
  else
    echo 'New changes to NOTICE.txt:'
    git --no-pager diff

    git add NOTICE.txt
    git commit -m"Update NOTICE.txt"
    git push

    exit 1
  fi
else
  echo 'Skipping autofix'
  make notice
  exit 0
fi
