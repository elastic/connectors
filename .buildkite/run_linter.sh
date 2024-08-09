#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

pyenv global $PYTHON_VERSION
echo "Python version:"
pyenv global

make lint
