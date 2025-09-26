#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module to responsible to generate jira documents using the Flask framework."""

import io
import os

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

PROJECT_TO_DELETE_COUNT = 100

match DATA_SIZE:
    case "small":
        projects_count = 500
    case "medium":
        projects_count = 2000
    case "large":
        projects_count = 10000


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
    projects_count -= (
        PROJECT_TO_DELETE_COUNT  # to delete 100 projects from Jira in next sync
    )
    return projects


@app.route("/rest/api/3/search/jql", methods=["GET"])
def get_all_issues():
    """Function to get all issues with pagination
    Returns:
        all_issues (dict): Dictionary of all issues
    """
    args = request.args
    max_results = int(args.get("maxResults", 100))
    next_page_token = args.get("nextPageToken")
    selected_fields = args.get("fields")

    all_issues = {"maxResults": max_results, "issues": []}

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

    if next_page_token == "null":
        error_msg = "next_page_token should not be 'null' string"
        raise Exception(error_msg)

    if next_page_token is None:
        for i in range(1, max_results + 1):
            all_issues["issues"].append(_compose_issue(i, selected_fields, fields))
        all_issues["nextPageToken"] = "second_page_token"
    elif next_page_token == "second_page_token":
        all_issues["issues"] = [{"id": "issue-101", "key": "DP-101", "fields": fields}]

    return all_issues


def _compose_issue(issue_id, selected_fields, fields):
    if selected_fields == "*all":
        return {
            "id": f"issue-{issue_id}",
            "key": f"DP-{issue_id}",
            "fields": fields,
        }
    elif selected_fields:
        return {
            "id": f"issue-{issue_id}",
            "key": f"DP-{issue_id}",
            "fields": {
                k: v for k, v in fields.items() if k in selected_fields.split(",")
            },
        }
    return {"id": f"issue-{issue_id}"}


@app.route("/rest/api/2/issue/<string:issue_id>", methods=["GET"])
def get_issue(issue_id):
    """Function to handle get issue calls with the id passed as argument
    Args:
        id (str): Issue id
    Returns:
        issue (dictionary): dictionary of issue data.
    """
    issue = {
        "id": issue_id,
        "key": f"DP-{issue_id}",
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
    return io.BytesIO(bytes(fake_provider.get_html(), encoding="utf-8"))


@app.route("/rest/api/2/field", methods=["GET"])
def get_fields():
    """Function to get all fields including default and custom fields from Jira"""
    return [
        {
            "clauseNames": ["description"],
            "custom": False,
            "id": "description",
            "name": "Description",
            "navigable": True,
            "orderable": True,
            "schema": {"system": "description", "type": "string"},
            "searchable": True,
        },
        {
            "clauseNames": ["summary"],
            "custom": True,
            "id": "customfield_001",
            "key": "summary",
            "name": "Summary",
            "navigable": True,
            "orderable": True,
            "schema": {"system": "summary", "type": "string"},
            "searchable": True,
        },
        {
            "clauseNames": ["author"],
            "custom": True,
            "id": "customfield_002",
            "key": "author",
            "name": "Author",
            "navigable": True,
            "orderable": True,
            "schema": {"system": "author", "type": "string"},
            "searchable": True,
        },
    ]


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
