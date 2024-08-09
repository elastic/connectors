#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile
pyenv global $PYTHON_VERSION
python3 --version
python --version
make install
bin/python --version
make test
