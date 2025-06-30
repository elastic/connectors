#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to responsible to generate Sandfly documents using the Flask framework."""

import os
from datetime import datetime, timedelta

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

fake = fake_provider.fake

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

match DATA_SIZE:
    case "small":
        HOSTS_COUNT = 3
        KEYS_COUNT = 7
        RESULTS_LOOP = 3
        RESULTS_COUNT = 10
    case "medium":
        HOSTS_COUNT = 5
        KEYS_COUNT = 11
        RESULTS_LOOP = 5
        RESULTS_COUNT = 150
    case "large":
        HOSTS_COUNT = 15
        KEYS_COUNT = 51
        RESULTS_LOOP = 11
        RESULTS_COUNT = 995
    case _:
        msg = f"Unknown DATA_SIZE: {DATA_SIZE}. Expecting 'small', 'medium' or 'large'"
        raise Exception(msg)


def get_num_docs():
    total_docs = HOSTS_COUNT + KEYS_COUNT + (RESULTS_LOOP * RESULTS_COUNT)
    print(total_docs)


class SandflyAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.results_start = 0
        self.results_stop = 0
        self.results_loop = 0

        self.app.route("/v4/auth/login", methods=["POST"])(self.get_access_token)
        self.app.route("/v4/license", methods=["GET"])(self.get_license)
        self.app.route("/v4/hosts", methods=["GET"])(self.get_hosts)
        self.app.route("/v4/sshhunter/summary", methods=["GET"])(self.get_ssh_summary)
        self.app.route("/v4/sshhunter/key/<string:key_id>", methods=["GET"])(self.get_ssh_key)
        self.app.route("/v4/results", methods=["POST"])(self.get_results)

    def get_access_token(self):
        return {
            "access_token": "Token#123",
            "refresh_token": "Refresh#123",
        }

    def get_license(self):
        def _format_date(date):
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")

        current_date = datetime.utcnow()

        return {
            "version": 3,
            "date": {"expiry": _format_date(current_date + timedelta(days=90))},
            "customer": {"name": fake.company()},
            "limits": {"features": ["demo", "elasticsearch_replication"]},
        }

    def get_hosts(self):
        return {
            "data": [
                {
                    "host_id": fake.sha1(),
                    "hostname": f"192.168.11.{host_id}",
                    "data": {"os": {"info": {"node": f"sandfly-target-{host_id}"}}},
                }
                for host_id in range(10, HOSTS_COUNT + 10)
            ],
        }

    def get_ssh_summary(self):
        return {
            "more_results": False,
            "data": [
                {"id": f"{key_id}"}
                for key_id in range(1, KEYS_COUNT + 1)
            ],
        }

    def get_ssh_key(self, key_id):
        return {
            "id": key_id,
            "friendly_name": f"key{key_id} " + fake.word() + " " + fake.word(),
            "key_value": f"KeyValue#123#{key_id}",
        }

    def get_results(self):
        def _format_date(date):
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")

        def _external_date(date):
            return date.strftime("%Y%m%d%H%M%SZ")

        current_date = datetime.utcnow()

        more_results = True

        if self.results_loop == 0:
            self.results_start = 1
            self.results_stop = RESULTS_COUNT + 1
        else:
            self.results_start += RESULTS_COUNT
            self.results_stop += RESULTS_COUNT

        self.results_loop += 1
        if self.results_loop >= RESULTS_LOOP:
            self.results_loop = 0
            more_results = False

        return {
            "more_results": more_results,
            "total": RESULTS_COUNT,
            "data": [
                {
                    "sequence_id": f"1000{result_id}",
                    "external_id": _external_date(current_date) + "." + fake.sha1(),
                    "header": {"end_time": _format_date(current_date)},
                    "data": {"key_data": fake.file_name(extension="sh"), "status": "alert"},
                }
                for result_id in range(self.results_start, self.results_stop)
            ],
        }


if __name__ == "__main__":
    SandflyAPI().app.run(host="0.0.0.0", port=8080)
