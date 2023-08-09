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
from random import choices
from faker import Faker
from functools import cached_property
from urllib.parse import parse_qs, urlparse

from flask import Flask, make_response, request

TABLE_FETCH_SIZE = 50

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

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

# TODO: change number of files based on DATA_SIZE
match DATA_SIZE:
    case "small":
        pass
    case "medium":
        pass
    case "large":
        pass

fake_provider = FakeProvider()

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]


def get_file():
    return choices(population, weights)[0]


class ServiceNowAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.table_length = 500
        self.get_table_length_call = 6
        self.files = {}
        self
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
        file = get_file()
        attachment_id = f"attachment-{table_sys_id}"
        self.files[attachment_id] = file
        record = [
            {
                "sys_id": attachment_id,
                "sys_updated_on": "2012-12-12",
                "size_bytes": len(file.encode("utf-8")),
                "file_name": f"{table_sys_id}.html",
            }
        ]
        return self.get_servicenow_formatted_data(
            response_key="result", response_data=record
        )

    def get_attachment_content(self, sys_id):
        file = self.files[sys_id]
        return io.BytesIO(bytes(file, encoding="utf-8"))


if __name__ == "__main__":
    ServiceNowAPI().app.run(host="0.0.0.0", port=9318)
