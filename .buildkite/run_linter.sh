#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

make lint

echo "Python version was:"
bin/python --version
