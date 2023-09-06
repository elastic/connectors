#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector."""

import io
import os
import random
import re
import string

from flask import Flask, request

RANDOMISER_CHARSET = string.ascii_letters + string.digits
DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]

SOBJECTS = [
    "Account",
    "Campaign",
    "Case",
    "CaseComment",
    "CaseFeed",
    "Contact",
    "ContentDocument",
    "ContentDocumentLink",
    "ContentVersion",
    "EmailMessage",
    "FeedComment",
    "Lead",
    "Opportunity",
    "User",
]
SOBJECT_FIELDS = [
    "AccountId",
    "BccAddress",
    "BillingAddress",
    "Body",
    "CaseNumber",
    "CcAddress",
    "CommentBody",
    "CommentCount",
    "Company",
    "ContentSize",
    "ConvertedAccountId",
    "ConvertedContactId",
    "ConvertedDate",
    "ConvertedOpportunityId",
    "Department",
    "Description",
    "Email",
    "EndDate",
    "FileExtension",
    "FirstOpenedDate",
    "FromAddress",
    "FromName",
    "IsActive",
    "IsClosed",
    "IsDeleted",
    "LastEditById",
    "LastEditDate",
    "LastModifiedById",
    "LatestPublishedVersionId",
    "LeadSource",
    "LinkUrl",
    "MessageDate",
    "Name",
    "OwnerId",
    "ParentId",
    "Phone",
    "PhotoUrl",
    "Rating",
    "StageName",
    "StartDate",
    "Status",
    "StatusParentId",
    "Subject",
    "TextBody",
    "Title",
    "ToAddress",
    "Type",
    "VersionDataUrl",
    "VersionNumber",
    "Website",
]
OBJECTS_WITH_CONTENT_DOCUMENTS = [
    "Account",
    "Campaign",
    "Case",
    "Contact",
    "Lead",
    "Opportunity",
]


def generate_string(size):
    "".join([random.choice(RANDOMISER_CHARSET) for _ in range(size)])


# We pre-generate 50 content document ids so we can randomly link them to multiple objects
# This will allow us to simulate the memory/CPU demand of objects having duplicate files attached
CONTENT_DOCUMENT_IDS = [generate_string(18) for _ in range(50)]
FILE_DATA = generate_string(FILE_SIZE)


def generate_records(table_name):
    records = []
    for _ in range(2000):
        record = {"Id": generate_string(18)}
        if table_name in OBJECTS_WITH_CONTENT_DOCUMENTS:
            record["ContentDocumentLinks"] = {
                "records": generate_content_document_records()
            }
        records.append(record)

    return records


def generate_content_document_records():
    # 1 in every 6 docs gets 1 attached file
    # 1 in every 6 docs gets 2 attached files
    d6 = random.choice(range(6))
    if d6 > 1:
        return []

    return [
        {
            "ContentDocument": {
                "Id": random.choice(CONTENT_DOCUMENT_IDS),
                "LatestPublishedVersion": {
                    "VersionDataUrl": f"http://localhost:10338/sfc/servlet.shepherd/version/download/{generate_string(18)}"
                },
            }
        }
        for _ in range(random.choice([1, 2]))
    ]


app = Flask(__name__)


@app.route("/services/oauth2/token", methods=["POST"])
def token():
    return {
        "access_token": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "signature": "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY",
        "instance_url": "http://127.0.0.1:10338",
        "id": "http://127.0.0.1:10338/id/ZZZZZZZZZZZZZZZZZZ/AAAAAAAAAAAAAAAAAA",
        "token_type": "Bearer",
        "issued_at": "1693822399207",
    }


@app.route("/services/data/<version>/query", methods=["GET"])
def query(version):
    query = request.args.get("q")
    table_name = re.findall(r"\bFROM\s+(\w+)", query)[-1]
    done = random.choice([True, False])

    response = {"done": done, "records": generate_records(table_name)}

    if not done:
        response["nextRecordsUrl"] = f"/services/data/{version}/query/{table_name}"

    return response


@app.route("/services/data/<_version>/query/<table_name>", methods=["GET"])
def query_next(_version, table_name):
    # table_name is supposed to be a followup query id.
    # We co-opt the value in ftests so we can track what table the queries are being run against,
    # as the table is not included in follow-up query params
    return {
        "done": True,
        "records": generate_records(table_name),
    }


@app.route("/services/data/<_version>/sobjects", methods=["GET"])
def describe(_version):
    return {"sobjects": [{"name": x, "queryable": True} for x in SOBJECTS]}


@app.route("/services/data/<_version>/sobjects/<_sobject>/describe", methods=["GET"])
def describe_sobject(_version, _sobject):
    return {"fields": [{"name": x} for x in SOBJECT_FIELDS]}


@app.route("/sfc/servlet.shepherd/version/download/<_download_id>", methods=["GET"])
def download(_download_id):
    return io.BytesIO(bytes(FILE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10338)
