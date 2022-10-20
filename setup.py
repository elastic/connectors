#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import sys
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise ValueError("Requires Python 3")
if sys.version_info.minor < 10:
    raise ValueError("Requires Python 3.10 or superior.")

from connectors import __version__  # NOQA


# We feed install_requires with `requirements.txt` but we unpin versions so we
# don't enforce them and trap folks into dependency hell. (only works with `==` here)
#
# A proper production installation will do the following sequence:
#
# $ pip install -r requirements.txt
# $ pip install elasticsearch-connectors
#
# Because the *pinned* dependencies is what we tested
#
install_requires = []
with open("requirements.txt") as f:
    reqs = f.readlines()
    for req in reqs:
        req = req.strip()
        if req == "" or req.startswith("#"):
            continue
        install_requires.append(req.split("=")[0])


with open("README.md") as f:
    long_description = f.read()


classifiers = [
    "Programming Language :: Python",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3 :: Only",
]


setup(
    name="elasticsearch-connectors",
    version=__version__,
    packages=find_packages(),
    description=("Elastic Search Connectors."),
    long_description=long_description,
    author="Ingestion Team",
    author_email="tarek@ziade.org",
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=install_requires,
    entry_points="""
      [console_scripts]
      elastic-ingest = connectors.cli:main
      fake-kibana = connectors.kibana:main
      """,
)
