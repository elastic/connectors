import os
import sys
from setuptools import find_packages, setup

if sys.version_info < (3, 10):
    msg = "Requires Python 3.10 or higher."
    raise ValueError(msg)

__version__ = "0.1.0"  # Define your version here or import from a version file


def read_reqs(req_file):
    with open(req_file) as f:
        return [line.strip() for line in f if line and not line.startswith("#")]


install_requires = read_reqs("../framework.txt")

with open("../README.md") as f:
    long_description = f.read()

classifiers = [
    "Programming Language :: Python",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3 :: Only",
]

setup(
    name="elastic_connectors",
    version=__version__,
    packages=find_packages(where="../package"),
    package_dir={"": "../package"},
    description=(
        "Connectors developed by Elastic to sync data from 3rd party sources."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ingest Team",
    author_email="your-email@elastic.co",
    include_package_data=True,
    zip_safe=False,
    classifiers=classifiers,
    install_requires=install_requires,
)
