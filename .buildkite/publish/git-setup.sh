#!/bin/bash
set -ex

export GIT_BRANCH=${BUILDKITE_BRANCH}

git switch -
git checkout $GIT_BRANCH
git pull origin $GIT_BRANCH
git config --local user.email 'elasticmachine@users.noreply.github.com'
git config --local user.name 'Elastic Machine'