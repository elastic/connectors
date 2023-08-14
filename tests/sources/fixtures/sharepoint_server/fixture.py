#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import io
import os
import random
import string

from flask import Flask, request
from flask_limiter import HEADERS, Limiter
from flask_limiter.util import get_remote_address

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

# Number of Sharepoint subsites
total_subsites = 201
# Number of Sharepoint lists
total_lists = 1000

SIZES = {
    "extra_small": 1048576,  # 1MB
    "small": 10485760,  # 10MB
    "medium": 20971520,  # 20MB
    "large": 99614720,  # 95MB
}
DOC_ID_SIZE = 36
DOC_ID_FILLING_CHAR = "0"  # used to fill in missing symbols for IDs

DATA_SIZE = os.environ.get("DATA_SIZE")

# if DATA_SIZE is passed then use only one file size
if DATA_SIZE and DATA_SIZE in SIZES:
    FILE_SIZES_DISTRIBUTION = {DATA_SIZE: 100}
else:
    FILE_SIZES_DISTRIBUTION = {
        "extra_small": 70,
        "small": 15,
        "medium": 10,
        "large": 5,
    }  # sum has to be 100


GENERATED_DATA = {}

# Generate range distribution (e.g. [range(0,2), range(2, 15), ...])
DISTRIBUTION_RANGES = {}
i = 0
for k, v in FILE_SIZES_DISTRIBUTION.items():
    DISTRIBUTION_RANGES[k] = range(i, i + v)
    i = i + v

# Generate data for different sizes
for k in FILE_SIZES_DISTRIBUTION.keys():
    GENERATED_DATA[k] = "".join(
        [random.choice(string.ascii_letters) for _ in range(SIZES[k])]
    )


def generate_attachment_data():
    rnd = random.randrange(100)

    for k, v in DISTRIBUTION_RANGES.items():
        if rnd in v:
            return io.BytesIO(bytes(GENERATED_DATA[k], encoding="utf-8"))

    # fallback to extra_small
    return io.BytesIO(bytes(GENERATED_DATA["extra_small"], encoding="utf-8"))


def adjust_document_id_size(document_id):
    """
    This methods make sure that all the documemts ids are min 36 bytes like in Sharepoint
    """

    bytesize = len(document_id)

    if bytesize >= DOC_ID_SIZE:
        return document_id

    addition = "".join(["0" for _ in range(DOC_ID_SIZE - bytesize - 1)])
    return f"{document_id}-{addition}"


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
    global total_lists
    lists = {"value": []}
    if "site1" in site:
        lists["value"].extend(
            [
                {
                    "BaseType": 1,
                    "Created": "2023-01-30T10:02:39Z",
                    "Id": adjust_document_id_size(f"document-library-{site}"),
                    "LastItemModifiedDate": "2023-01-30T10:02:40Z",
                    "ParentWebUrl": f"/{parent_site_url}",
                    "Title": f"{site}-List2",
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
    else:
        skip_start = int(request.args.get("$skip"))
        top = int(request.args.get("$top"))
        if skip_start + top >= total_lists:
            skip_end = total_lists
            # Removing the data for the second sync
            total_lists -= 50
        else:
            skip_end = skip_start + top

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
    if list_id == "lists-site1-0":
        item = {
            "value": [
                {
                    "Attachments": True,
                    "AttachmentFiles": [
                        {
                            "FileName": f"dummy{list_id}.txt",
                            "ServerRelativeUrl": parent_site_url,
                        }
                    ],
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
    elif args.get("$expand", "") == "AttachmentFiles":
        item = {
            "value": [
                {
                    "Attachments": False,
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": adjust_document_id_size(
                        f"list-item-{parent_site_url}-{list_id}"
                    ),
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "AuthorId": 12345,
                    "EditorId": 12345,
                    "Title": f"list-item-{list_id}",
                    "Id": adjust_document_id_size(f"list-id1-{list_id}"),
                    "ContentTypeId": f"123-{list_id}",
                },
                {
                    "Attachments": False,
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": adjust_document_id_size(f"list-item-{list_id}"),
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "AuthorId": 12345,
                    "EditorId": 12345,
                    "Title": f"list-item-{list_id}",
                    "Id": adjust_document_id_size(f"list-id2-{list_id}"),
                    "ContentTypeId": f"456-{list_id}",
                },
            ]
        }
    else:
        item = {
            "value": [
                {
                    "Folder": {
                        "Name": "Folder sharepoint",
                        "ServerRelativeUrl": f"{parent_site_url}/Shared Documents/Folder sharepoint",
                        "TimeCreated": "2023-02-13T10:24:36Z",
                        "TimeLastModified": "2023-02-13T10:24:36Z",
                    },
                    "Modified": "2023-02-13T10:24:36Z",
                    "GUID": adjust_document_id_size(
                        f"drive-folder-{parent_site_url}-{list_id}"
                    ),
                },
                {
                    "File": {
                        "Length": "26949",
                        "Name": f"dummy{list_id}.txt",
                        "ServerRelativeUrl": parent_site_url,
                        "TimeCreated": "2023-01-30T10:02:40Z",
                        "TimeLastModified": "2023-01-30T10:02:40Z",
                        "Title": f"folder-{parent_site_url}",
                        "UniqueId": "6f885b24-af40-44e0-bd53-82e76e634cf6",
                    },
                    "Modified": "2023-01-30T10:02:39Z",
                    "GUID": adjust_document_id_size(
                        f"drive-file-{parent_site_url}-{list_id}"
                    ),
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
        "Length": "26949",
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
    return generate_attachment_data()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8491)
