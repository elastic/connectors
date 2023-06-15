#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate jira documents using the Flask framework.
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
projects_count = 1000


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
    projects_count = 900  # to delete 100 projects from Jira in next sync
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
    return io.BytesIO(bytes(LARGE_DATA, encoding="utf-8"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
