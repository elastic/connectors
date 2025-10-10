#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
# ruff: noqa: T201
"""Module responsible to generate GitLab documents using the Flask framework."""

import base64
import os
from datetime import datetime, timezone

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium").lower()

# Configure data size
match DATA_SIZE:
    case "small":
        PROJECT_COUNT = 5
        ISSUES_PER_PROJECT = 100
        MRS_PER_PROJECT = 50
    case "medium":
        PROJECT_COUNT = 10
        ISSUES_PER_PROJECT = 500
        MRS_PER_PROJECT = 250
    case "large":
        PROJECT_COUNT = 20
        ISSUES_PER_PROJECT = 1000
        MRS_PER_PROJECT = 500


class GitLabAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.project_count = PROJECT_COUNT
        self.issues_per_project = ISSUES_PER_PROJECT
        self.mrs_per_project = MRS_PER_PROJECT
        self.app.route("/api/graphql", methods=["POST"])(self.mock_graphql_response)
        self.app.route("/api/v4/user", methods=["GET"])(self.get_user)
        self.app.route(
            "/api/v4/projects/<int:project_id>/repository/tree", methods=["GET"]
        )(self.get_repository_tree)
        self.app.route(
            "/api/v4/projects/<path:project_path>/repository/files/<path:file_path>",
            methods=["GET"],
        )(self.get_file_content)

    def encode_cursor(self, value):
        """Encode cursor for pagination."""
        return base64.b64encode(str(value).encode()).decode()

    def decode_cursor(self, cursor):
        """Decode cursor for pagination."""
        if not cursor:
            return 0
        try:
            return int(base64.b64decode(cursor.encode()).decode())
        except Exception:
            return 0

    def get_page(self, data, cursor, page_size=100):
        """Get a page of data based on cursor."""
        start_index = self.decode_cursor(cursor)
        end_index = start_index + page_size
        subset = data[start_index:end_index]
        has_next = end_index < len(data)
        return subset, has_next, self.encode_cursor(end_index) if has_next else None

    def get_user(self):
        """Mock user endpoint for authentication check."""
        return {
            "id": 1,
            "username": "test-user",
            "name": "Test User",
            "state": "active",
        }

    def get_repository_tree(self, project_id):
        """Mock repository tree endpoint for README discovery."""
        # Return a simple tree with just a README file
        return [
            {
                "id": f"readme_{project_id}",
                "name": "README.md",
                "type": "blob",
                "path": "README.md",
                "mode": "100644",
            }
        ]

    def mock_graphql_response(self):
        """Mock GitLab GraphQL API responses."""
        data = request.get_json()
        query = data.get("query", "")
        variables = data.get("variables", {})

        # Clean query for easier matching
        query_normalized = query.replace(" ", "").replace("\n", "").replace("\r", "")

        # Projects query
        if "projects(membership:true" in query_normalized:
            return self.mock_projects(variables)

        # Issues query (legacy API, not used anymore but kept for reference)
        if (
            "project(fullPath:$projectPath)" in query_normalized
            and "issues(" in query_normalized
        ):
            return self.mock_issues(variables)

        # Merge requests query
        if "mergeRequests(" in query_normalized:
            return self.mock_merge_requests(variables)

        # Work items project query
        if "workItems(" in query_normalized and "project(fullPath:" in query_normalized:
            return self.mock_work_items_project(variables)

        # Work item full query (project-level)
        if (
            "workItem(id:" in query_normalized
            and "project(fullPath:" in query_normalized
        ):
            return self.mock_work_item_full(variables)

        # Work items group query
        if "workItems(" in query_normalized and "group(fullPath:" in query_normalized:
            return self.mock_work_items_group(variables)

        # Work item full query (group-level)
        if "workItem(id:" in query_normalized and "group(fullPath:" in query_normalized:
            return self.mock_work_item_group_full(variables)

        # Releases query
        if "releases(" in query_normalized:
            return self.mock_releases(variables)

        # Remaining fields (assignees, labels, discussions, notes)
        if "assignees(" in query_normalized:
            return self.mock_remaining_assignees(variables)
        if "labels(" in query_normalized:
            return self.mock_remaining_labels(variables)
        if "discussions(" in query_normalized:
            return self.mock_remaining_discussions(variables)
        if "notes(" in query_normalized:
            return self.mock_remaining_notes(variables)

        return {"errors": [{"message": "Unknown query type"}]}

    def mock_projects(self, variables):
        """Mock projects list."""
        projects = [
            {
                "id": f"gid://gitlab/Project/{i}",
                "name": f"Test Project {i}",
                "path": f"test-project-{i}",
                "fullPath": f"test-group/test-project-{i}",
                "description": fake_provider.fake.text(max_nb_chars=200),
                "visibility": "public",
                "starCount": i * 10,
                "forksCount": i * 2,
                "createdAt": "2023-01-01T00:00:00Z",
                "lastActivityAt": datetime.now(timezone.utc).isoformat(),
                "archived": False,
                "webUrl": f"https://gitlab.com/test-group/test-project-{i}",
                "repository": {"rootRef": "main"},
                "group": {"id": "gid://gitlab/Group/1", "fullPath": "test-group"},
            }
            for i in range(1, self.project_count + 1)
        ]

        subset, has_next, end_cursor = self.get_page(
            projects, variables.get("cursor"), 100
        )

        return {
            "data": {
                "projects": {
                    "pageInfo": {
                        "hasNextPage": has_next,
                        "endCursor": end_cursor,
                    },
                    "nodes": subset,
                }
            }
        }

    def mock_issues(self, variables):
        """Mock issues list (legacy API - not actively used)."""
        project_path = variables.get("projectPath", "")
        issues = [
            {
                "id": f"gid://gitlab/Issue/{i}",
                "iid": str(i),
                "title": f"Issue {i}",
                "description": fake_provider.fake.text(max_nb_chars=500),
                "state": "opened" if i % 2 == 0 else "closed",
                "createdAt": "2023-06-01T00:00:00Z",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "webUrl": f"https://gitlab.com/{project_path}/-/issues/{i}",
                "author": {
                    "username": "test-user",
                    "name": "Test User",
                },
                "assignees": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "labels": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "discussions": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
            for i in range(1, self.issues_per_project + 1)
        ]

        subset, has_next, end_cursor = self.get_page(
            issues, variables.get("cursor"), 100
        )

        return {
            "data": {
                "project": {
                    "issues": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "nodes": subset,
                    }
                }
            }
        }

    def mock_merge_requests(self, variables):
        """Mock merge requests list."""
        project_path = variables.get("projectPath", "")
        mrs = [
            {
                "id": f"gid://gitlab/MergeRequest/{i}",
                "iid": str(i),
                "title": f"Merge Request {i}",
                "description": fake_provider.fake.text(max_nb_chars=500),
                "state": "opened" if i % 3 == 0 else "merged",
                "createdAt": "2023-06-01T00:00:00Z",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "mergedAt": "2023-06-15T00:00:00Z" if i % 3 != 0 else None,
                "webUrl": f"https://gitlab.com/{project_path}/-/merge_requests/{i}",
                "sourceBranch": f"feature/branch-{i}",
                "targetBranch": "main",
                "author": {
                    "username": "test-user",
                    "name": "Test User",
                },
                "assignees": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "reviewers": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "approvedBy": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "labels": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "discussions": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
            }
            for i in range(1, self.mrs_per_project + 1)
        ]

        subset, has_next, end_cursor = self.get_page(mrs, variables.get("cursor"), 100)

        return {
            "data": {
                "project": {
                    "mergeRequests": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "nodes": subset,
                    }
                }
            }
        }

    def mock_work_items_project(self, variables):
        """Mock work items list for a project."""
        work_item_types = variables.get("types", ["ISSUE"])
        project_path = variables.get("projectPath", "test-group/test-project")
        items = [
            {
                "id": f"gid://gitlab/WorkItem/{i}",
                "iid": str(i),
                "title": f"Work Item {i}",
                "state": "OPEN" if i % 2 == 0 else "CLOSED",
                "createdAt": "2023-06-01T00:00:00Z",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "webUrl": f"https://gitlab.com/{project_path}/-/work_items/{i}",
                "workItemType": {
                    "name": work_item_types[0] if work_item_types else "ISSUE"
                },
            }
            for i in range(1, self.issues_per_project + 1)
        ]

        subset, has_next, end_cursor = self.get_page(items, variables.get("cursor"), 50)

        return {
            "data": {
                "project": {
                    "workItems": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "nodes": subset,
                    }
                }
            }
        }

    def mock_work_item_full(self, variables):
        """Mock full work item details."""
        work_item_id = variables.get("id", "")
        iid_match = work_item_id.split("/")[-1] if "/" in work_item_id else "1"
        project_path = variables.get("projectPath", "test-group/test-project")

        return {
            "data": {
                "project": {
                    "workItem": {
                        "id": work_item_id,
                        "iid": iid_match,
                        "title": f"Work Item {iid_match}",
                        "state": "OPEN",
                        "createdAt": "2023-06-01T00:00:00Z",
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                        "workItemType": {"name": "ISSUE"},
                        "author": {
                            "username": "test-user",
                            "name": "Test User",
                        },
                        "webUrl": f"https://gitlab.com/{project_path}/-/work_items/{iid_match}",
                        "widgets": [
                            {
                                "__typename": "WorkItemWidgetDescription",
                                "type": "DESCRIPTION",
                                "description": fake_provider.fake.text(
                                    max_nb_chars=500
                                ),
                            },
                            {
                                "__typename": "WorkItemWidgetAssignees",
                                "type": "ASSIGNEES",
                                "assignees": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                            {
                                "__typename": "WorkItemWidgetLabels",
                                "type": "LABELS",
                                "labels": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                            {
                                "__typename": "WorkItemWidgetNotes",
                                "type": "NOTES",
                                "discussions": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                        ],
                    }
                }
            }
        }

    def mock_work_items_group(self, variables):
        """Mock work items list for a group (epics)."""
        # Epics are fewer in number
        epic_count = max(10, self.issues_per_project // 10)
        work_item_types = variables.get("types", ["EPIC"])
        group_path = variables.get("groupPath", "test-group")

        items = [
            {
                "id": f"gid://gitlab/WorkItem/epic-{i}",
                "iid": str(i),
                "title": f"Epic {i}",
                "state": "OPEN" if i % 2 == 0 else "CLOSED",
                "createdAt": "2023-06-01T00:00:00Z",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
                "webUrl": f"https://gitlab.com/groups/{group_path}/-/epics/{i}",
                "workItemType": {
                    "name": work_item_types[0] if work_item_types else "EPIC"
                },
            }
            for i in range(1, epic_count + 1)
        ]

        subset, has_next, end_cursor = self.get_page(items, variables.get("cursor"), 50)

        return {
            "data": {
                "group": {
                    "workItems": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "nodes": subset,
                    }
                }
            }
        }

    def mock_work_item_group_full(self, variables):
        """Mock full work item details for group-level (epics)."""
        work_item_id = variables.get("id", "")
        iid_match = work_item_id.split("/")[-1] if "/" in work_item_id else "1"

        return {
            "data": {
                "group": {
                    "workItem": {
                        "id": work_item_id,
                        "iid": iid_match,
                        "title": f"Epic {iid_match}",
                        "state": "OPEN",
                        "createdAt": "2023-06-01T00:00:00Z",
                        "updatedAt": datetime.now(timezone.utc).isoformat(),
                        "workItemType": {"name": "EPIC"},
                        "author": {
                            "username": "test-user",
                            "name": "Test User",
                        },
                        "webUrl": f"https://gitlab.com/groups/test-group/-/epics/{iid_match}",
                        "widgets": [
                            {
                                "__typename": "WorkItemWidgetDescription",
                                "type": "DESCRIPTION",
                                "description": fake_provider.fake.text(
                                    max_nb_chars=500
                                ),
                            },
                            {
                                "__typename": "WorkItemWidgetAssignees",
                                "type": "ASSIGNEES",
                                "assignees": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                            {
                                "__typename": "WorkItemWidgetLabels",
                                "type": "LABELS",
                                "labels": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                            {
                                "__typename": "WorkItemWidgetNotes",
                                "type": "NOTES",
                                "discussions": {
                                    "nodes": [],
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": None,
                                    },
                                },
                            },
                        ],
                    }
                }
            }
        }

    def mock_releases(self, variables):
        """Mock releases list."""
        releases = [
            {
                "name": f"v1.{i}.0",
                "tagName": f"v1.{i}.0",
                "description": fake_provider.fake.text(max_nb_chars=300),
                "createdAt": "2023-06-01T00:00:00Z",
                "releasedAt": "2023-06-01T00:00:00Z",
                "author": {
                    "username": "test-user",
                    "name": "Test User",
                },
                "commit": {
                    "sha": fake_provider.fake.sha1(),
                },
                "milestones": {
                    "nodes": [],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                },
                "assets": {
                    "count": 0,
                    "links": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    },
                },
            }
            for i in range(1, 6)  # Small number of releases
        ]

        subset, has_next, end_cursor = self.get_page(
            releases, variables.get("cursor"), 100
        )

        return {
            "data": {
                "project": {
                    "releases": {
                        "pageInfo": {
                            "hasNextPage": has_next,
                            "endCursor": end_cursor,
                        },
                        "nodes": subset,
                    }
                }
            }
        }

    def mock_remaining_assignees(self, variables):
        """Mock remaining assignees pagination."""
        return {
            "data": {
                "node": {
                    "assignees": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    def mock_remaining_labels(self, variables):
        """Mock remaining labels pagination."""
        return {
            "data": {
                "node": {
                    "labels": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    def mock_remaining_discussions(self, variables):
        """Mock remaining discussions pagination."""
        return {
            "data": {
                "node": {
                    "discussions": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    def mock_remaining_notes(self, variables):
        """Mock remaining notes pagination."""
        return {
            "data": {
                "node": {
                    "notes": {
                        "nodes": [],
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                    }
                }
            }
        }

    def get_file_content(self, project_path, file_path):
        """Mock file content retrieval via REST API."""
        # Generate a fake README
        content = fake_provider.fake.text(max_nb_chars=2000)
        encoded_content = base64.b64encode(content.encode()).decode()

        return {
            "file_name": file_path,
            "file_path": file_path,
            "size": len(content),
            "encoding": "base64",
            "content": encoded_content,
            "ref": "main",
        }


def get_num_docs():
    """Calculate expected document count."""
    # Projects + (Issues + MRs + Releases + README per project) + (Epics per group)
    epics_count = max(10, ISSUES_PER_PROJECT // 10)
    releases_per_project = 5
    readme_per_project = 1

    docs_per_project = (
        ISSUES_PER_PROJECT + MRS_PER_PROJECT + releases_per_project + readme_per_project
    )
    total = PROJECT_COUNT + (PROJECT_COUNT * docs_per_project) + epics_count

    print(total)


if __name__ == "__main__":
    GitLabAPI().app.run(host="0.0.0.0", port=9091, ssl_context="adhoc")
