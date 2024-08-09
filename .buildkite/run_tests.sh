#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

pyenv global $PYTHON_VERSION
make test

echo "Python version was:"
bin/python --version
