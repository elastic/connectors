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

app = Flask(__name__)
start_lists, end_lists = 0, 450
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 1000000, "medium": 2000000, "large": 6000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])


@app.route("/sites/<string:site_collections>/_api/web/webs", methods=["GET"])
def get_sites(site_collections):
    """Function to handle get sites calls with the site_collection passed as an argument
    Args:
        site_collections (str): Path of collection

    Returns:
        sites (str): Path of server site
    """
    args = request.args
    sites = {"value": []}
    if request.path == "/sites/site1/_api/web/webs":
        return sites
    elif args.get("$skip"):
        sites["value"].append(
            {
                "Created": "2023-01-30T10:02:39",
                "Id": "sites-0",
                "LastItemModifiedDate": "2023-02-16T06:48:30Z",
                "ServerRelativeUrl": "/sites/site1",
                "Title": "site1",
                "Url": f"http://127.0.0.1:8000/sites/{site_collections}/site1",
            }
        )
    else:
        sites = {
            "value": [
                {
                    "Created": "2023-01-30T10:02:39",
                    "Id": "ping-1aaabbb",
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
    global start_lists, end_lists
    lists = {"value": []}
    for lists_count in range(start_lists, end_lists):
        lists["value"].extend(
            [
                {
                    "BaseType": 0,
                    "Created": "2023-01-30T10:02:39Z",
                    "Id": f"lists-{site}-{lists_count}",
                    "LastItemModifiedDate": "2023-01-30T10:02:40Z",
                    "ParentWebUrl": f"/{parent_site_url}",
                    "Title": "List1",
                    "RootFolder": {
                        "Name": "Shared Documents",
                        "ServerRelativeUrl": f"/{parent_site_url}/{site}",
                        "TimeCreated": "2023-02-08T06:03:10Z",
                        "TimeLastModified": "2023-02-16T06:48:36Z",
                        "UniqueId": "52e62bcf-de67-4bbc-b399-b8b28bc97449",
                    },
                },
                {
                    "BaseType": 1,
                    "Created": "2023-01-30T10:02:39Z",
                    "Id": f"document-library-{site}-{lists_count}",
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
    # Removing the data for the second sync
    end_lists -= 50
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
                    "EditorId": "aabb-112c",
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
                    "GUID": f"list-item-{parent_site_url}-{list_id}",
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "EditorId": "aabb-112c",
                    "Title": f"list-item-{list_id}",
                    "Id": f"list-id1-{list_id}",
                    "ContentTypeId": f"123-{list_id}",
                },
                {
                    "Attachments": False,
                    "Created": "2023-01-30T10:02:39Z",
                    "GUID": f"list-item-{list_id}",
                    "FileRef": parent_site_url,
                    "Modified": "2023-01-30T10:02:40Z",
                    "EditorId": "aabb-112c",
                    "Title": f"list-item-{list_id}",
                    "Id": f"list-id2-{list_id}",
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
                    "GUID": f"drive-folder-{parent_site_url}-{list_id}",
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
                    "GUID": f"drive-file-{parent_site_url}-{list_id}",
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
    return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8491)
