#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate ServiceNow documents using the Flask framework.
"""
import io
import os
import random
import string

from flask import Flask, request

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])
record_count = 500
total_services = 5

app = Flask(__name__)


@app.route("/api/now/table/<string:table>", methods=["GET"])
def get_table_data(table):
    """Get table data

    Args:
        table (str): Table name

    Returns:
        dict: Table data
    """

    global record_count, total_services
    records = []
    if request.args["sysparm_query"] == "ORDERBYsys_created_on^label=Incident":
        records.append({"name": "incident", "label": "Incident"})
    elif int(request.args["sysparm_offset"]) == 0:
        for i in range(record_count):
            records.append(
                {
                    "sys_id": f"{table}-{i}",
                    "sys_updated_on": "1212-12-12",
                    "sys_class_name": table,
                }
            )
    elif int(request.args["sysparm_offset"]) == 500:
        for i in range(record_count, record_count * 2):
            records.append(
                {
                    "sys_id": f"{table}-{i}",
                    "sys_updated_on": "1212-12-12",
                    "sys_class_name": table,
                }
            )
    elif int(request.args["sysparm_offset"]) == 1000:
        total_services -= 1
        if total_services == 0:
            record_count = 800  # to delete 200 records per service in next sync
    return {"result": records}


@app.route("/api/now/attachment", methods=["GET"])
def get_attachment_data():
    """Get attachment data

    Returns:
        dict: Attachment data
    """

    record = []
    if not int(request.args["sysparm_offset"]) == 1:
        record = [
            {
                "sys_id": f"attachment-{request.args['table_sys_id']}",
                "sys_updated_on": "1212-12-12",
                "size_bytes": FILE_SIZE,
                "file_name": f"{request.args['table_sys_id']}.txt",
            }
        ]
    return {"result": record}


@app.route("/api/now/attachment/<string:sys_id>/file", methods=["GET"])
def get_attachment_content(sys_id):
    """Get attachment content

    Args:
        sys_id (str): Table id

    Returns:
        io.BytesIO: Attachment content
    """

    return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9318)
