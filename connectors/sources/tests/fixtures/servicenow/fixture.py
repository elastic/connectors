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


class ServiceNowAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.record_count = 500
        self.total_services = 5
        self.app.route("/api/now/table/<string:table>", methods=["GET"])(
            self.get_table_data
        )
        self.app.route("/api/now/attachment", methods=["GET"])(self.get_attachment_data)
        self.app.route("/api/now/attachment/<string:sys_id>/file", methods=["GET"])(
            self.get_attachment_content
        )

    def get_table_data(self, table):
        """Get table data

        Args:
            table (str): Table name

        Returns:
            dict: Table data
        """

        records = []
        is_incident_sorted_by_creation_date_query = (
            request.args["sysparm_query"] == "ORDERBYsys_created_on^label=Incident"
        )
        if is_incident_sorted_by_creation_date_query:
            records.append({"name": "incident", "label": "Incident"})
        elif int(request.args["sysparm_offset"]) == 0:
            for i in range(self.record_count):
                records.append(
                    {
                        "sys_id": f"{table}-{i}",
                        "sys_updated_on": "1212-12-12",
                        "sys_class_name": table,
                    }
                )
        elif int(request.args["sysparm_offset"]) == 500:
            for i in range(self.record_count, self.record_count * 2):
                records.append(
                    {
                        "sys_id": f"{table}-{i}",
                        "sys_updated_on": "1212-12-12",
                        "sys_class_name": table,
                    }
                )
        elif int(request.args["sysparm_offset"]) == 1000:
            self.total_services -= 1
            if self.total_services == 0:
                self.record_count = (
                    800  # to delete 200 records per service in next sync
                )
        return {"result": records}

    def get_attachment_data(self):
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

    def get_attachment_content(self, sys_id):
        """Get attachment content

        Args:
            sys_id (str): Table id

        Returns:
            io.BytesIO: Attachment content
        """

        return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    ServiceNowAPI().app.run(host="0.0.0.0", port=9318)
