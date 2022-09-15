#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import sys
from setuptools import setup, find_packages

if sys.version_info.major != 3:
    raise ValueError("Requires Python 3")
if sys.version_info.minor < 6:
    raise ValueError("Requires Python 3.6 or superior")

from connectors import __version__  # NOQA

install_requires = []

description = ""

for file_ in ("README",):
    with open("%s.rst" % file_) as f:
        description += f.read() + "\n\n"


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
