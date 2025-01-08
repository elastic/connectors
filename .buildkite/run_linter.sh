#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh
source .buildkite/publish/git-setup.sh

init_python

if is_pr; then
  echo "We're on PR, running autoformat"
  if ! make autoformat ; then
    echo "make autoformat ran with errors, exiting"
    exit 1
  fi

  if [ -z "$(git status --porcelain)" ]; then
    echo "Nothing to be fixed by autoformat"
    exit 0
  else
    # We lint and if it succeeds, we can push the change
    if make lint ; then
      echo "linting errors are fixed, pushing the diff"
      export GH_TOKEN="$VAULT_GITHUB_TOKEN"

      git add .
      git commit -m"make autoformat"
      git push
    else
      # Needs manual work
      echo "make autoformat did not fix all linting errors, exiting"
      exit 1
    fi
  fi
else
  echo "We're not on PR, running only linter"
  # On non-PR branches the bot has no permissions to open PRs.
  # Theoretically this would never fail because we always ask
  # linter to succeed to merge. It can fail intermittently?
  make lint
  return
fi
