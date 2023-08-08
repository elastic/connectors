#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate jira documents using the Flask framework.
"""
import io
import os
from random import choices
from faker import Faker
from functools import cached_property
import string

from flask import Flask, request

# TODO: make used generally
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

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

PROJECT_TO_DELETE_COUNT = 100

match DATA_SIZE:
    case "small":
        projects_count = 500
    case "medium":
        projects_count = 2000
    case "large":
        projects_count = 10000

population = [fake_provider.small_html(), fake_provider.medium_html(), fake_provider.large_html(), fake_provider.extra_large_html()]
weights = [0.58, 0.3, 0.1, 0.02]

def get_file():
    return choices(population, weights)[0]

app = Flask(__name__)


@app.route("/rest/api/2/myself", methods=["GET"])
def get_myself():
    """Function to load an authenticated user's data"""
    myself = {
        "accountId": "5ff5815e34847e0069fedee3",
        "emailAddress": "test.user@gmail.com",
        "displayName": "Test User",
        "timeZone": "Asia/Calcutta",
    }
    return myself


@app.route("/rest/api/2/project", methods=["GET"])
def get_projects():
    """Function to load projects on the jira server
    Returns:
        projects (list): List of projects
    """
    global projects_count
    projects = []
    for i in range(1, projects_count + 1):
        projects.append(
            {"id": f"project {i}", "key": f"DP-{i}", "name": f"Demo Project {i}"}
        )
    projects_count -= PROJECT_TO_DELETE_COUNT  # to delete 100 projects from Jira in next sync
    return projects


@app.route("/rest/api/2/search", methods=["GET"])
def get_all_issues():
    """Function to get all issues with pagination
    Returns:
        all_issues (dict): Dictionary of all issues
    """
    args = request.args
    all_issues = {"startAt": 0, "maxResults": 100, "total": 2000, "issues": []}
    fields = {
        "issuetype": {"name": "Task"},
        "project": {"key": "DP", "name": "Demo Project"},
        "fixVersions": [],
        "attachment": [
            {
                "id": 10001,
                "filename": "dummy_file.txt",
                "size": 200,
                "created": "2023-02-09T08:33:57.284+0000",
            }
        ],
        "priority": {"name": "Medium"},
        "assignee": "Test User",
        "updated": "2023-02-10T00:10:11.027+0530",
        "status": {"name": "To Do"},
        "summary": "Dummy Issue",
        "reporter": {"emailAddress": "test.user@gmail.com", "displayName": "Test User"},
    }

    if args["maxResults"] == "100" and args["startAt"] != "":
        start_at = int(args["startAt"])
        all_issues["startAt"] = start_at
        if start_at + int(args["maxResults"]) > all_issues["total"]:
            return all_issues
        for i in range(start_at + 1, start_at + 101):
            all_issues["issues"].append(
                {"id": f"issue-{i}", "key": f"DP-{i}", "fields": fields}
            )
    return all_issues


@app.route("/rest/api/2/issue/<string:id>", methods=["GET"])
def get_issue(id):
    """Function to handle get issue calls with the id passed as argument
    Args:
        id (str): Issue id
    Returns:
        issue (dictionary): dictionary of issue data.
    """
    issue = {
        "id": id,
        "key": f"DP-{id}",
        "fields": {
            "issuetype": {"name": "Task"},
            "project": {"key": "DP", "name": "Demo Project"},
            "fixVersions": [],
            "attachment": [
                {
                    "id": 10001,
                    "filename": "dummy_file.txt",
                    "size": 200,
                    "created": "2023-02-09T08:33:57.284+0000",
                }
            ],
            "priority": {"name": "Medium"},
            "assignee": "Test User",
            "updated": "2023-02-10T00:10:11.027+0530",
            "status": {"name": "To Do"},
            "summary": "Dummy Issue",
            "reporter": {
                "emailAddress": "test.user@gmail.com",
                "displayName": "Test User",
            },
        },
    }
    return issue


@app.route("/rest/api/2/attachment/content/<string:attachment_id>", methods=["GET"])
def get_attachment_content(attachment_id):
    """Function to handle get attachment content calls
    Args:
        id (string): id of an attachment.
    Returns:
        data_reader (io.BytesIO): Dummy attachment content
    """
    return io.BytesIO(bytes(get_file(), encoding="utf-8"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
