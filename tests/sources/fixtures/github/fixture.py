#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to responsible to generate GitHub documents using the Flask framework.
"""
import base64
import os

from flask import Flask, make_response, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

# TODO: change number of files based on DATA_SIZE
match DATA_SIZE:
    case "small":
        FILE_COUNT = 500
        ISSUE_COUNT = 100
    case "medium":
        FILE_COUNT = 3500
        ISSUE_COUNT = 1000
    case "large":
        FILE_COUNT = 35000
        ISSUE_COUNT = 5000


app = Flask(__name__)


def encode_data(content):
    return base64.b64encode(bytes(content, "utf-8")).decode("utf-8")


class GitHubAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.file_count = FILE_COUNT
        self.issue_count = ISSUE_COUNT
        self.app.route(
            "/api/v3/repos/demo_user/demo_repo/git/trees/main", methods=["GET"]
        )(self.get_tree)
        self.app.route(
            "/api/v3/repos/demo_user/demo_repo/git/blobs/<string:file_id>",
            methods=["GET"],
        )(self.get_content)
        self.app.route("/api/v3/repos/demo_user/demo_repo/commits", methods=["GET"])(
            self.get_commits
        )
        self.app.route("/api/graphql", methods=["POST"])(self.mock_graphql_response)
        self.files = {}

    def encode_cursor(self, value):
        return base64.b64encode(str(value).encode()).decode()

    def decode_cursor(self, cursor):
        return int(base64.b64decode(cursor.encode()).decode())

    def get_index_metadata(self, variables, data):
        after = variables.get("cursor")
        start_index = self.decode_cursor(after) if after else 0
        end_index = start_index + 100
        subset_nodes = data["nodes"][start_index:end_index]
        return start_index, end_index, subset_nodes

    def mock_graphql_response(self):
        data = request.get_json()
        query = data.get("query")
        variables = data.get("variables", {})

        repos_data = {
            "nodes": [
                {
                    "id": "1234",
                    "updatedAt": "2023-04-19T09:32:54Z",
                    "name": "demo_repo",
                    "nameWithOwner": "demo_user/demo_repo",
                    "url": "https://github.com/demo_user/demo_repo",
                    "description": "This is test Description",
                    "visibility": "Public",
                    "primaryLanguage": {
                        "name": "Python",
                    },
                    "defaultBranchRef": {"name": "main"},
                    "isFork": True,
                    "stargazerCount": 10,
                    "watchers": {
                        "totalCount": 10,
                    },
                    "forkCount": 10,
                    "createdAt": "2023-04-19T09:32:54Z",
                }
            ],
            "pageInfo": {
                "hasNextPage": False,
                "hasPreviousPage": False,
                "startCursor": "",
                "endCursor": "",
            },
        }
        pull_request_data = {
            "nodes": [
                {
                    "title": f"Pull Request {pull_number}",
                    "name": f"Node {pull_number}",
                    "number": pull_number,
                    "id": f"pull_request_{pull_number}",
                    "updatedAt": "2023-04-20T09:32:54Z",
                    "comments": {
                        "nodes": [
                            {
                                "author": {
                                    "login": "demo_user",
                                },
                                "body": "This is comments",
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": "abcd123",
                        },
                    },
                    "labels": {
                        "nodes": [
                            {
                                "name": "enhancement",
                                "description": "This is label",
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd123"},
                    },
                    "reviewRequests": {
                        "nodes": [
                            {
                                "requestedReviewer": {
                                    "login": "test_user",
                                }
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd123"},
                    },
                    "assignees": {
                        "nodes": [
                            {
                                "login": "demo_user",
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd123"},
                    },
                    "reviews": {
                        "nodes": [
                            {
                                "state": "COMMENTED",
                                "author": {
                                    "login": "demo_user",
                                },
                                "body": "This is comments",
                                "comments": {
                                    "nodes": [
                                        {"body": "This is comment"},
                                    ]
                                },
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": "abcd123",
                        },
                    },
                }
                for pull_number in range(self.issue_count)
            ],
            "pageInfo": {
                "hasNextPage": False,
                "hasPreviousPage": False,
                "startCursor": "",
                "endCursor": "",
            },
        }
        issue_data = {
            "nodes": [
                {
                    "title": f"issue {issue_number}",
                    "number": issue_number,
                    "id": f"issue_{issue_number}",
                    "updatedAt": "2023-04-20T09:32:54Z",
                    "comments": {
                        "nodes": [
                            {
                                "author": {
                                    "login": "demo_user",
                                },
                                "body": "This is comments",
                            }
                        ],
                        "pageInfo": {
                            "hasNextPage": False,
                            "endCursor": "abcd123",
                        },
                    },
                    "labels": {
                        "nodes": [
                            {
                                "name": "enhancement",
                                "description": "This is label",
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd123"},
                    },
                    "assignees": {
                        "nodes": [
                            {
                                "login": "demo_user",
                            }
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd123"},
                    },
                }
                for issue_number in range(self.issue_count)
            ],
            "pageInfo": {
                "hasNextPage": False,
                "hasPreviousPage": False,
                "startCursor": "",
                "endCursor": "",
            },
        }

        mock_data = {}
        query = query.replace(" ", "").replace("\r", "").replace("\n", "")
        if "viewer{login}" in query:
            mock_data = make_response({"data": {"viewer": {"login": "demo_repo"}}})
            mock_data.status_code = 200
            mock_data.headers["X-OAuth-Scopes"] = ["repo"]
        elif "repositories" in query:
            start_index, end_index, subset_nodes = self.get_index_metadata(
                variables, repos_data
            )
            mock_data["data"] = {
                "user": {
                    "repositories": {
                        "nodes": subset_nodes,
                        "pageInfo": {
                            "hasNextPage": end_index < len(repos_data["nodes"]),
                            "hasPreviousPage": start_index > 0,
                            "startCursor": self.encode_cursor(start_index + 1),
                            "endCursor": self.encode_cursor(end_index),
                        },
                    }
                }
            }
        elif "pullRequests" in query:
            start_index, end_index, subset_nodes = self.get_index_metadata(
                variables, pull_request_data
            )
            mock_data["data"] = {
                "repository": {
                    "pullRequests": {
                        "nodes": subset_nodes,
                        "pageInfo": {
                            "hasNextPage": end_index < len(pull_request_data["nodes"]),
                            "hasPreviousPage": start_index > 0,
                            "startCursor": self.encode_cursor(start_index + 1),
                            "endCursor": self.encode_cursor(end_index),
                        },
                    }
                }
            }
        elif "issues" in query:
            start_index, end_index, subset_nodes = self.get_index_metadata(
                variables, issue_data
            )
            mock_data["data"] = {
                "repository": {
                    "issues": {
                        "nodes": subset_nodes,
                        "pageInfo": {
                            "hasNextPage": end_index < len(issue_data["nodes"]),
                            "hasPreviousPage": start_index > 0,
                            "startCursor": self.encode_cursor(start_index + 1),
                            "endCursor": self.encode_cursor(end_index),
                        },
                    }
                }
            }
        else:
            return {"errors": ["Invalid query."]}

        return mock_data

    def get_tree(self):
        args = request.args
        tree_list = []
        if args.get("recursive") == "1":
            for file_number in range(self.file_count):
                file = fake_provider.get_html()
                self.files[str(file_number)] = file
                tree_list.append(
                    {
                        "path": f"dummy_file_{file_number}.md",
                        "mode": "100644",
                        "type": "blob",
                        "sha": file_number,
                        "size": len(file.encode("utf-8")),
                        "url": f"http://127.0.0.1:9091/api/v3/repos/demo_user/demo_repo/git/blobs/{file_number}",
                    }
                )
        self.file_count = 2000
        return {"tree": tree_list}

    def get_content(self, file_id):
        file = self.files[file_id]
        return {
            "name": f"dummy_file_{file_id}.md",
            "path": f"dummy_file_{file_id}.md",
            "sha": file_id,
            "size": len(file.encode("utf-8")),
            "type": "file",
            "content": encode_data(file),
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
