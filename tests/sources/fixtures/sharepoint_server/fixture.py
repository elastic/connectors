#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import io
import os
import random
import time
from random import choices
import string

from faker import Faker
from flask import Flask, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address
from functools import cached_property

app = Flask(__name__)

THROTTLING = os.environ.get("THROTTLING", False)

if THROTTLING:
    limiter = Limiter(
        get_remote_address,
        app=app,
        storage_uri="memory://",
        application_limits=[
            "6000 per minute",
            "6000000 per day",
        ],  # Sharepoint 50k+ licences limits
        retry_after="delta_seconds",
        headers_enabled=True,
        header_name_mapping={
            HEADERS.LIMIT: "RateLimit-Limit",
            HEADERS.RESET: "RateLimit-Reset",
            HEADERS.REMAINING: "RateLimit-Remaining",
        },
    )

DOC_ID_SIZE = 36
DOC_ID_FILLING_CHAR = "0"  # used to fill in missing symbols for IDs

DATA_SIZE = os.environ.get("DATA_SIZE")

class FakeProvider:
    def __init__(self, seed=None):
        self.seed = seed
        self.fake = Faker()
        if seed:
            self.fake.seed_instance(seed)

    @cached_property
    def _cached_random_str(self):
        return self.fake.pystr(min_chars=100 * 1024, max_chars=100 * 1024 + 1)

    def small_text(self):
        # Up to 1KB of text
        return self.generate_text(1 * 1024)

    def medium_text(self):
        # Up to 1MB of text
        return self.generate_text(1024 * 1024)

    def large_text(self):
        # Up to 4MB of text
        return self.generate_text(4 * 1024 * 1024)

    def extra_large_text(self):
        return self.generate_text(20 * 1024 * 1024)

    def small_html(self):
        # Around 100KB
        return self.generate_html(1)

    def medium_html(self):
        # Around 1MB
        return self.generate_html(1 * 10)

    def large_html(self):
        # Around 8MB
        return self.generate_html(8 * 10)

    def extra_large_html(self):
        # Around 25MB
        return self.generate_html(25 * 10)

    def generate_text(self, max_size):
        return self.fake.text(max_nb_chars=max_size)

    def generate_html(self, images_of_100kb):
        img = self._cached_random_str  # 100kb
        text = self.small_text()

        images = []
        for _ in range(images_of_100kb):
            images.append(f"<img src='{img}'/>")

        return f"<html><head></head><body><div>{text}</div><div>{'<br/>'.join(images)}</div></body></html>"


fake_provider = FakeProvider()
fake = fake_provider.fake

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]


def get_file():
    return choices(population, weights)[0]


def adjust_document_id_size(id):
    """
    This methods make sure that all the documemts ids are min 36 bytes like in Sharepoint
    """

    bytesize = len(id)

    if bytesize >= DOC_ID_SIZE:
        return id

    addition = "".join(["0" for _ in range(DOC_ID_SIZE - bytesize - 1)])
    return f"{id}-{addition}"


match DATA_SIZE:
    case "small":
        total_subsites = 1
        lists_per_site = 10
        attachments_per_list = 5
    case "medium":
        total_subsites = 10
        lists_per_site = 40
        attachments_per_list = 15
    case "large":
        total_subsites = 100
        lists_per_site = 1000
        attachments_per_list = 40

def get_num_docs():
    print(total_subsites * (1 + lists_per_site * (1 + attachments_per_list)))

PRE_REQUEST_SLEEP = 0.1

@app.before_request
def before_request():
    time.sleep(PRE_REQUEST_SLEEP)

@app.route("/sites/<string:site_collections>/_api/web/webs", methods=["GET"])
def get_sites(site_collections):
    """Function to handle get sites calls with the site_collection passed as an argument
    Args:
        site_collections (str): Path of collection

    Returns:
        sites (str): Path of server site
    """
    skip = request.args.get("$skip")
    sites = {"value": []}
    if "/sites/site1" in request.path:
        return sites
    if skip:
        skip_start = int(skip)
        top = int(request.args.get("$top"))
        if skip_start + top >= total_subsites:
            skip_end = total_subsites
        else:
            skip_end = skip_start + top
        for site in range(skip_start, skip_end):
            sites["value"].append(
                {
                    "Created": "2023-01-30T10:02:39",
                    "Id": adjust_document_id_size(f"sites-{site}"),
                    "LastItemModifiedDate": "2023-02-16T06:48:30Z",
                    "ServerRelativeUrl": f"/sites/site1_{site}",
                    "Title": f"site1_{site}",
                    "Url": f"http://127.0.0.1:8000/sites/{site_collections}/site1_{site}",
                }
            )
    else:
        sites = {
            "value": [
                {
                    "Created": "2023-01-30T10:02:39",
                    "Id": adjust_document_id_size("ping-1aaabbb"),
                    "LastItemModifiedDate": "2023-02-16T06:48:30Z",
                    "ServerRelativeUrl": "/sites/collection1/site1",
                    "Title": "site1",
                    "Url": f"http://127.0.0.1:8000/sites/{site_collections}/site1",
                },
            ]
        }
    return sites


@app.route("/<string:parent_site_url>/<string:site>/_api/web/lists", methods=["GET"])
def get_lists(parent_site_url, site):
    """Function to handle get lists calls with the serversiteurl passed as an argument
    Args:
        parent_site_url (str): Path of parent site
        site (str): Site name

    Returns:
        lists (dict): Dictionary of lists
    """
    lists = { "value": [] }
    top = int(request.args.get("$top"))
    skip_start = int(request.args.get("$skip"))
    skip_end = min(lists_per_site, skip_start + top)

    for lists_count in range(skip_start, skip_end):
        lists["value"].extend(
            [
                {
                    "BaseType": 0,
                    "Created": "2023-01-30T10:02:39Z",
                    "Id": adjust_document_id_size(f"lists-{site}-{lists_count}"),
                    "LastItemModifiedDate": "2023-01-30T10:02:40Z",
                    "ParentWebUrl": f"/{parent_site_url}",
                    "Title": f"{site}-List1",
                    "RootFolder": {
                        "Name": "Shared Documents",
                        "ServerRelativeUrl": f"/{parent_site_url}/{site}",
                        "TimeCreated": "2023-02-08T06:03:10Z",
                        "TimeLastModified": "2023-02-16T06:48:36Z",
                        "UniqueId": "52e62bcf-de67-4bbc-b399-b8b28bc97449",
                    },
                },
            ]
        )

    return lists


@app.route(
    "/<string:parent_site_url>/_api/web/lists(guid'<string:list_id>')/items",
    methods=["GET"],
)
def get_list_and_items(parent_site_url, list_id):
    """Function to handle get drive and list item calls in sharepoint
    Args:
        parent_site_url (str): Path of parent site
        list_id (str): Id of list

    Returns:
        item (dict): Dictionary of list item or drive item
    """
    args = request.args
    if args.get("$expand", "") == "AttachmentFiles":
        item = {
            "value": [
                {
                    "Attachments": True,
                    "AttachmentFiles": [],
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": f"list-item-att-{parent_site_url}-{list_id}",
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "AuthorId": 12345,
                    "EditorId": 12345,
                    "Title": f"list-item-{list_id}",
                }
            ]
        }
        for item_id in range(attachments_per_list):
            name = f"dummy{list_id}-{item_id}.html"
            item["value"][0]["AttachmentFiles"].append({
                "FileName": f"dummy{list_id}-{item_id}.txt",
                "ServerRelativeUrl": parent_site_url,
            })
    else:
        item = {
            "value": [
                {
                    "Attachments": False,
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": adjust_document_id_size(
                        f"list-item-{parent_site_url}-{list_id}"
                    ),
                    "Modified": "2023-01-30T10:02:40Z",
                    "AuthorId": 12345,
                    "EditorId": 12345,
                    "Title": f"list-item-{list_id}",
                    "Id": adjust_document_id_size(f"{parent_site_url}-list-id1-{list_id}"),
                    "ContentTypeId": f"123-{list_id}",
                },
                {
                    "Attachments": False,
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": adjust_document_id_size(f"{parent_site_url}-list-item-{list_id}"),
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "AuthorId": 12345,
                    "EditorId": 12345,
                    "Title": f"list-item-{list_id}",
                    "Id": adjust_document_id_size(f"{parent_site_url}-list-id2-{list_id}"),
                    "ContentTypeId": f"456-{list_id}",
                },
            ]
        }

    return item


@app.route(
    "/<string:parent_site_url>/_api/web/getfilebyserverrelativeurl('<string:file_relative_url>')",
    methods=["GET"],
)
def get_attachment_data(parent_site_url, file_relative_url):
    """Function to fetch attachment data on the sharepoint
    Args:
        parent_site_url (str): Path of parent site
        file_relative_url (str): Path of attachment file

    Returns:
        data (dict): Dictionary of attachment metadata
    """
    return {
        "Length": 12345,
        "Name": f"attachment-{parent_site_url}",
        "ServerRelativeUrl": f"{parent_site_url}/dummy",
        "TimeCreated": "2023-01-30T10:02:40Z",
        "TimeLastModified": "2023-01-30T10:02:40Z",
        "UniqueId": f"attachment-{parent_site_url}",
    }


@app.route(
    "/<string:parent_url>/<string:site>/_api/web/GetFileByServerRelativeUrl('<string:server_url>')/$value",
    methods=["GET"],
)
def download(parent_url, site, server_url):
    """Function to extract content of a attachment on the sharepoint
    Args:
        parent_url (str): Path of parent site
        site (str): Name of site
        server_url (str): Server relative url of site

    Returns:
        data_reader (io.BytesIO): object of io.BytesIO.
    """
    file = get_file()
    return file.encode("utf-8")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8491)
