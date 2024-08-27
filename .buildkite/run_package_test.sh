#!/bin/bash

# !!! WARNING DO NOT add -x to avoid leaking vault passwords
set -euo pipefail

source ~/.bash_profile

pyenv global $PYTHON_VERSION
echo "Python version:"
pyenv global

make sdist
cd dist
python -m venv .venv

# Install the package locally
.venv/bin/pip install  $(ls | grep tar)

# Just run version checks
.venv/bin/elastic-ingest --version
.venv/bin/connectors --version
