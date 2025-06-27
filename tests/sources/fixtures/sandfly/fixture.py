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
        RESULTS_COUNT = 10
    case "medium":
        HOSTS_COUNT = 5
        RESULTS_COUNT = 150
    case "large":
        HOSTS_COUNT = 15
        RESULTS_COUNT = 995
    case _:
        msg = f"Unknown DATA_SIZE: {DATA_SIZE}. Expecting 'small', 'medium' or 'large'"
        raise Exception(msg)


class SandflyAPI:
    def __init__(self):
        self.app = Flask(__name__)

        self.app.route("/v4/auth/login", methods=["POST"])(self.get_access_token)
        self.app.route("/v4/license", methods=["GET"])(self.get_license)
        self.app.route("/v4/hosts", methods=["GET"])(self.get_hosts)
        self.app.route("/v4/sshhunter/summary", methods=["GET"])(self.get_ssh_summary)
        self.app.route("/v4/sshhunter/key/1", methods=["GET"])(self.get_ssh_key1)
        self.app.route("/v4/sshhunter/key/2", methods=["GET"])(self.get_ssh_key2)
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
            "customer": {"name": "Sandfly"},
            "limits": {"features": ["demo", "elasticsearch_replication"]},
        }

    def get_hosts(self):
        return {
            "data": [
                {
                    "host_id": f"100{host_id}",
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
                {"id": "1"},
                {"id": "2"},
            ],
        }

    def get_ssh_key1(self):
        return {
            "id": "1",
            "friendly_name": "a b c",
            "key_value": "KeyValue#123",
        }

    def get_ssh_key2(self):
        return {
            "id": "2",
            "friendly_name": "d e f",
            "key_value": "KeyValue#456",
        }

    def get_results(self):
        def _format_date(date):
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")

        current_date = datetime.utcnow()

        return {
            "more_results": False,
            "total": 2,
            "data": [
                {
                    "sequence_id": "1003",
                    "external_id": "1003",
                    "header": {"end_time": _format_date(current_date)},
                    "data": {"key_data": "my key data", "status": "alert"},
                },
                {
                    "sequence_id": "1004",
                    "external_id": "1004",
                    "header": {"end_time": _format_date(current_date)},
                    "data": {"key_data": "", "status": "alert"},
                },
            ],
        }


if __name__ == "__main__":
    SandflyAPI().app.run(host="0.0.0.0", port=8080)
