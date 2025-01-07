#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source .buildkite/shared.sh

init_python

if is_pr; then
  echo "We're on PR, running autoformat"
  make autoformat

  if [ -z "$(git status --porcelain)" ]; then
    exit 0
  else
    # We lint and if it succeeds, we can push the change
    if make lint ; then
      # Command succeeded, can push
      gh pr checkout "${BUILDKITE_PULL_REQUEST}"
      git add .
      git commit -m"make autoformat"
      git push
    else
      # Needs manual work
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
