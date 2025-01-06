#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

pyenv global $PYTHON_VERSION
echo "Python version:"
pyenv global

make notice

if [ -z "$(git status --porcelain | grep NOTICE.txt)" ]; then
  exit 0
else 
  git add NOTICE.txt
  git commit -m"Update NOTICE.txt"
  git push
  exit 2
fi
