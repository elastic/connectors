#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate ServiceNow documents using the Flask framework.
"""
import base64
import io
import os
import random
import string
from urllib.parse import parse_qs, urlparse

from flask import Flask, make_response, request

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])
TABLE_FETCH_SIZE = 50


class ServiceNowAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.table_length = 500
        self.get_table_length_call = 6
        self.app.route("/api/now/table/<string:table>", methods=["GET"])(
            self.get_table_length
        )
        self.app.route("/api/now/attachment/<string:sys_id>/file", methods=["GET"])(
            self.get_attachment_content
        )
        self.app.route("/api/now/v1/batch", methods=["POST"])(self.get_batch_data)

    def get_servicenow_formatted_data(self, response_key, response_data):
        return bytes(str({response_key: response_data}).replace("'", '"'), "utf-8")

    def get_url_data(self, url):
        parsed_url = urlparse(url)
        return parsed_url.path, parse_qs(parsed_url.query)

    def decode_response(self, response):
        return base64.b64encode(response).decode()

    def get_batch_data(self):
        batch_data = request.get_json()
        response = []
        for rest_request in batch_data["rest_requests"]:
            path, query_params = self.get_url_data(url=rest_request["url"])
            last_endpoint = path.split("/")[-1]
            if last_endpoint == "attachment":
                table_sys_id = query_params["table_sys_id"]
                attachment_data = self.get_attachment_data(table_sys_id=table_sys_id[0])
                response.append(
                    {
                        "body": self.decode_response(response=attachment_data),
                        "status_code": 200,
                    }
                )
            else:
                table_name = last_endpoint
                table_offset = query_params["sysparm_offset"]
                table_data = self.get_table_data(
                    table=table_name, offset=int(table_offset[0])
                )
                response.append(
                    {
                        "body": self.decode_response(response=table_data),
                        "status_code": 200,
                    }
                )
        batch_response = make_response(
            self.get_servicenow_formatted_data(
                response_key="serviced_requests", response_data=response
            )
        )
        batch_response.headers["Content-Type"] = "application/json"
        return batch_response

    def get_table_length(self, table):
        response = make_response(bytes(str({"Response": "Dummy"}), "utf-8"))
        response.headers["Content-Type"] = "application/json"
        if int(request.args["sysparm_limit"]) == 1:
            response.headers["x-total-count"] = self.table_length
        else:
            response.headers["x-total-count"] = 0
        self.get_table_length_call -= 1
        if self.get_table_length_call == 0:
            self.table_length = 300  # to delete 2000 records per service in next sync
        return response

    def get_table_data(self, table, offset):
        records = []
        for i in range(offset - TABLE_FETCH_SIZE, offset):
            records.append(
                {
                    "sys_id": f"{table}-{i}",
                    "sys_updated_on": "1212-12-12",
                    "sys_class_name": table,
                }
            )
        return self.get_servicenow_formatted_data(
            response_key="result", response_data=records
        )

    def get_attachment_data(self, table_sys_id):
        record = [
            {
                "sys_id": f"attachment-{table_sys_id}",
                "sys_updated_on": "1212-12-12",
                "size_bytes": FILE_SIZE,
                "file_name": f"{table_sys_id}.txt",
            }
        ]
        return self.get_servicenow_formatted_data(
            response_key="result", response_data=record
        )

    def get_attachment_content(self, sys_id):
        return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    ServiceNowAPI().app.run(host="0.0.0.0", port=9318)
