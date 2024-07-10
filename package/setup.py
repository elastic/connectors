import os
from setuptools import setup, find_packages

from setuptools._vendor.packaging.markers import Marker

try:
    ARCH = os.uname().machine
except Exception as e:
    ARCH = "x86_64"
    print(  # noqa: T201
        f"Defaulting to architecture '{ARCH}'. Unable to determine machine architecture due to error: {e}"
    )


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


install_requires = read_reqs(
    os.path.join(os.path.dirname(__file__), f"../requirements/{ARCH}.txt")
)

print(find_packages(include=["elastic_connectors", "elastic_connectors.*"]))

setup(
    name="test-elastic-connectors",
    version="0.1.3",
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    package_data={"elastic_connectors": ["../connectors/*"]},
    package_dir={
        "elastic_connectors": ".",
    },
)
