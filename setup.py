#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import sys
import os
from setuptools import setup, find_packages


try:
    ARCH = os.uname().machine
except Exception:
    ARCH = "x86_64"

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
# $ pip install -r requirements/`uname -n`.txt
# $ pip install elasticsearch-connectors
#
# Because the *pinned* dependencies is what we tested
#


def read_reqs(req_file):
    deps = []
    reqs_dir, __ = os.path.split(req_file)

    with open(req_file) as f:
        reqs = f.readlines()
        for req in reqs:
            req = req.strip()
            if req == "" or req.startswith("#"):
                continue
            if req.startswith("-r"):
                subreq_file = req.split("-r")[-1].strip()
                subreq_file = os.path.join(reqs_dir, subreq_file)
                for subreq in read_reqs(subreq_file):
                    dep = subreq.split("=")[0]
                    if dep not in deps:
                        deps.append(dep)
            else:
                dep = req.split("=")[0]
                if dep not in deps:
                    deps.append(dep)
    return deps


install_requires = read_reqs(os.path.join("requirements", f"{ARCH}.txt"))


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
