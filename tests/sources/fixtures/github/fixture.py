#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate GitHub documents using the Flask framework.
"""
import base64
import os
import random
import string

from flask import Flask, make_response, request

DATA_SIZE = os.environ.get("DATA_SIZE", "small").lower()
_SIZES = {"small": 500000, "medium": 1000000, "large": 3000000}
FILE_SIZE = _SIZES[DATA_SIZE]
LARGE_DATA = "".join([random.choice(string.ascii_letters) for _ in range(FILE_SIZE)])
DELETED_PAGES = "12"
FIRST_PAGE = "1"
ALL_TYPE = "all"


app = Flask(__name__)


def encode_data(content):
    return base64.b64encode(bytes(content, "utf-8")).decode("utf-8")


class GitHubAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.pull_request_pages = "15"
        self.issue_pages = "15"
        self.file_count = 2500
        self.app.route("/user", methods=["GET"])(self.get_user)
        self.app.route("/user/repos", methods=["GET"])(self.get_repos)
        self.app.route("/repos/demo_user/demo_repo/pulls", methods=["GET"])(
            self.get_pull_requests
        )
        self.app.route(
            "/repos/demo_user/demo_repo/issues/<string:id>/comments", methods=["GET"]
        )(self.get_comments)
        self.app.route(
            "/repos/demo_user/demo_repo/pulls/<string:id>/comments", methods=["GET"]
        )(self.get_comments)
        self.app.route(
            "/repos/demo_user/demo_repo/pulls/<string:id>/reviews", methods=["GET"]
        )(self.get_reviews)
        self.app.route("/repos/demo_user/demo_repo/issues", methods=["GET"])(
            self.get_issues
        )
        self.app.route("/repos/demo_user/demo_repo/git/trees/main", methods=["GET"])(
            self.get_tree
        )
        self.app.route(
            "/repos/demo_user/demo_repo/git/blobs/<string:commit_id>", methods=["GET"]
        )(self.get_content)
        self.app.route("/repos/demo_user/demo_repo/commits", methods=["GET"])(
            self.get_commits
        )

    def get_user(self):
        response = make_response(
            {
                "login": "demo_user",
                "id": "user_1",
            }
        )
        response.status_code = 200
        response.headers["X-OAuth-Scopes"] = ["repo"]
        return response

    def get_repos(self):
        args = request.args
        if args.get("type") == ALL_TYPE and args.get("page") == FIRST_PAGE:
            return [
                {
                    "id": "repo_1",
                    "updated_at": "2023-04-19T09:32:54Z",
                    "name": "demo_repo",
                    "full_name": "demo_user/demo_repo",
                    "html_url": "https://github.com/demo_user/demo_repo",
                    "owner": {"login": "demo_user"},
                    "description": "this is a test repository",
                    "visibility": "public",
                    "language": "Python",
                    "fork": "false",
                    "open_issues": 4,
                    "default_branch": "main",
                    "forks_count": 2,
                    "watchers": 10,
                    "stargazers_count": 10,
                    "created_at": "2023-04-19T09:32:54Z",
                }
            ]
        if args.get("page") == "2":
            return []

    def get_pull_requests(self):
        args = request.args
        if args.get("state") == ALL_TYPE:
            pulls = [
                {
                    "id": f"pulls_{args['page']}_{pull_number}",
                    "updated_at": "2023-04-24T08:44:00Z",
                    "number": pull_number,
                    "html_url": f"https://github.com/demo_user/demo_repo/pull/{pull_number}",
                    "created_at": "2023-04-24T08:44:00Z",
                    "closed_at": "2023-04-25T08:44:00Z",
                    "title": "demo txt",
                    "state": "open",
                    "merged_at": "2023-04-25T08:44:00Z",
                    "merge_commit_sha": "2123bad7d8sfsa4fds54",
                    "body": "Update hello world",
                    "user": {"login": "demo_user"},
                    "head": {"label": "test-branch"},
                    "base": {"label": "main"},
                    "assignees": [{"login": "demo_user"}],
                    "requested_reviewers": [{"login": "test_user"}],
                    "requested_teams": [{"name": "team1"}],
                    "labels": [{"name": "V8.8"}],
                }
                for pull_number in range(100)
            ]
        if args.get("page") == self.pull_request_pages:
            self.pull_request_pages = DELETED_PAGES
            return []
        return pulls

    def get_comments(self, id):
        args = request.args
        comments = [{"body": "demo comments", "user": {"login": "demo_user"}}]
        return comments if args.get("page") == FIRST_PAGE else []

    def get_reviews(self, id):
        args = request.args
        comments = [
            {
                "body": "demo comments",
                "user": {"login": "demo_user"},
                "state": "COMMENTED",
            }
        ]
        return comments if args.get("page") == FIRST_PAGE else []

    def get_issues(self):
        args = request.args
        if args.get("state") == ALL_TYPE:
            issues = [
                (
                    {
                        "id": f"issues_{args['page']}_{issue_number}",
                        "updated_at": "2023-04-20T08:56:23Z",
                        "number": issue_number,
                        "html_url": "https://github.com/demo_user/demo_repo/issues/1",
                        "created_at": "2023-04-19T08:56:23Z",
                        "closed_at": "2023-04-20T08:56:23Z",
                        "title": "demo issues",
                        "body": "demo issues test",
                        "state": "open",
                        "user": {"login": "demo_user"},
                        "assignees": [{"login": "demo_user"}],
                        "labels": [{"name": "bug"}],
                    }
                )
                for issue_number in range(100)
            ]
        if args.get("page") == self.issue_pages:
            self.issue_pages = DELETED_PAGES
            return []
        return issues

    def get_tree(self):
        args = request.args
        if args.get("recursive") == FIRST_PAGE:
            tree_list = [
                {
                    "path": f"dummy_file_{file_number}.txt",
                    "mode": "100644",
                    "type": "blob",
                    "sha": file_number,
                    "size": 30,
                    "url": f"http://127.0.0.1:9091/repos/demo_user/demo_repo/git/blobs/{file_number}",
                }
                for file_number in range(self.file_count)
            ]
        self.file_count = 2000
        return {"tree": tree_list}

    def get_content(self, commit_id):
        return {
            "name": f"dummy_file_{commit_id}.txt",
            "path": f"dummy_file_{commit_id}.txt",
            "sha": commit_id,
            "size": 5545,
            "type": "file",
            "content": encode_data(LARGE_DATA),
            "encoding": "base64",
        }

    def get_commits(self):
        return [
            {
                "commit": {
                    "committer": {
                        "name": "GitHub",
                        "email": "noreply@github.com",
                        "date": "2023-04-17T12:55:01Z",
                    },
                },
            }
        ]


if __name__ == "__main__":
    GitHubAPI().app.run(host="0.0.0.0", port=9091)
