#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
import os
import random
import shutil
import urllib.request
import zipfile

SYSTEM_DIR = os.path.join(os.path.dirname(__file__), "data")
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()

if DATA_SIZE == "small":
    REPO = "connectors-python"
elif DATA_SIZE == "medium":
    REPO = "elasticsearch"
else:
    REPO = "kibana"


def get_num_docs():
    match os.environ.get("DATA_SIZE", "medium"):
        case "small":
            print("100")
        case "medium":
            print("200")
        case _:
            print("300")


async def load():
    if os.path.exists(SYSTEM_DIR):
        teardown()
    print(f"Working in {SYSTEM_DIR}")
    os.makedirs(SYSTEM_DIR)
    repo_zip = os.path.join(SYSTEM_DIR, "repo.zip")

    # lazy tree generator: we download the elasticsearch repo and unzip it
    print(f"Downloading some source from {REPO} this may take a while...")
    urllib.request.urlretrieve(
        f"https://github.com/elastic/{REPO}/zipball/main", repo_zip
    )

    print("Unzipping the tree")
    with zipfile.ZipFile(repo_zip) as zip_ref:
        zip_ref.extractall(SYSTEM_DIR)

    os.unlink(repo_zip)


async def remove():
    # removing 10 files
    files = []
    for root, __, filenames in os.walk(SYSTEM_DIR):
        for filename in filenames:
            files.append(os.path.join(root, filename))

    random.shuffle(files)
    for i in range(10):
        print(f"deleting {files[i]}")
        os.unlink(files[i])


async def teardown():
    shutil.rmtree(SYSTEM_DIR)
