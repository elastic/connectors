#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

from setuptools import find_packages, setup
from setuptools._vendor.packaging.markers import Marker

try:
    ARCH = os.uname().machine
except Exception as e:
    ARCH = "x86_64"
    print(  # noqa: T201
        f"Defaulting to architecture '{ARCH}'. Unable to determine machine architecture due to error: {e}"
    )


def extract_req(req):
    req = req.strip().split(";")
    if len(req) > 1:
        env_marker = req[-1].strip()
        marker = Marker(env_marker)
        if not marker.evaluate():
            return None
    req = req[0]
    req = req.split("=")
    return req[0]


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
                    dep = extract_req(subreq)
                    if dep is not None and dep not in deps:
                        deps.append(dep)
            else:
                dep = extract_req(req)
                if dep is not None and dep not in deps:
                    deps.append(dep)
    return deps


framework_reqs = read_reqs(
    os.path.join("elastic_connectors", "requirements", f"{ARCH}.txt")
)
package_reqs = read_reqs(
    os.path.join("elastic_connectors", "requirements", "package.txt")
)

install_requires = framework_reqs + package_reqs


with open("README.md") as f:
    long_description = f.read()

setup(
    name="elastic-connectors",
    version="0.2.0",
    description="Elastic connectors",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Jedr Blaszyk",
    author_email="jedr.blaszyk@elastic.co",
    url="https://github.com/elastic/connectors",
    packages=find_packages(include=["elastic_connectors", "elastic_connectors.*"]),
    install_requires=install_requires,
    include_package_data=True,
    package_data={
        "elastic_connectors": ["connectors/VERSION"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
