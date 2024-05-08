#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Github source class methods"""
from contextlib import asynccontextmanager
from copy import deepcopy
from unittest.mock import ANY, AsyncMock, Mock, patch

import aiohttp
import pytest
from aiohttp.client_exceptions import ClientResponseError
from gidgethub.abc import GraphQLAuthorizationFailure, QueryError

from connectors.access_control import DLS_QUERY
from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Features, Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.github import (
    GITHUB_APP,
    PERSONAL_ACCESS_TOKEN,
    REPOSITORY_OBJECT,
    ForbiddenException,
    GitHubAdvancedRulesValidator,
    GitHubDataSource,
    UnauthorizedException,
)
from tests.commons import AsyncIterator
from tests.sources.support import create_source

ADVANCED_SNIPPET = "advanced_snippet"


def public_repo():
    return {
        "name": "demo_repo",
        "nameWithOwner": "demo_user/demo_repo",
        "url": "https://github.com/demo_user/demo_repo",
        "description": "Demo repo for poc",
        "visibility": "PUBLIC",
        "primaryLanguage": {"name": "Python"},
        "defaultBranchRef": {"name": "main"},
        "isFork": False,
        "stargazerCount": 0,
        "watchers": {"totalCount": 1},
        "forkCount": 0,
        "createdAt": "2023-04-17T06:06:25Z",
        "_id": "R_kgDOJXuc8A",
        "_timestamp": "2023-06-20T07:09:34Z",
        "isArchived": False,
        "type": "Repository",
    }


def pull_request():
    return {
        "data": {
            "repository": {
                "pullRequests": {
                    "nodes": [
                        {
                            "id": "1",
                            "updatedAt": "2023-07-03T12:24:16Z",
                            "number": 2,
                            "url": "https://github.com/demo_repo/demo_repo/pull/2",
                            "createdAt": "2023-04-19T09:06:01Z",
                            "closedAt": "None",
                            "title": "test pull request",
                            "body": "test pull request",
                            "state": "OPEN",
                            "mergedAt": "None",
                            "author": {
                                "login": "author-authorson",
                            },
                            "assignees": {
                                "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                                "nodes": [{"login": "test_user"}],
                            },
                            "labels": {
                                "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                                "nodes": [
                                    {
                                        "name": "bug",
                                        "description": "Something isn't working",
                                    },
                                ],
                            },
                            "reviewRequests": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "abcd1234",
                                },
                                "nodes": [
                                    {"requestedReviewer": {"login": "test_user"}}
                                ],
                            },
                            "comments": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "Y3Vyc29yOnYyOpHOXmz8gA==",
                                },
                                "nodes": [
                                    {
                                        "author": {"login": "test_user"},
                                        "body": "issues comments",
                                    },
                                ],
                            },
                            "reviews": {
                                "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                                "nodes": [
                                    {
                                        "author": {"login": "test_user"},
                                        "state": "COMMENTED",
                                        "body": "add some comments",
                                        "comments": {
                                            "pageInfo": {
                                                "hasNextPage": False,
                                                "endCursor": "abcd",
                                            },
                                            "nodes": [{"body": "nice!!!"}],
                                        },
                                    },
                                ],
                            },
                        }
                    ]
                }
            }
        }
    }


def issue():
    return {
        "data": {
            "repository": {
                "issues": {
                    "nodes": [
                        {
                            "number": 1,
                            "url": "https://github.com/demo_user/demo_repo/issues/1",
                            "createdAt": "2023-04-18T10:12:21Z",
                            "closedAt": None,
                            "title": "demo issues",
                            "body": "demo issues test",
                            "state": "OPEN",
                            "id": "I_kwDOJXuc8M5jtMsK",
                            "type": "Issue",
                            "updatedAt": "2023-04-19T08:56:23Z",
                            "comments": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": "abcd4321",
                                },
                                "nodes": [
                                    {
                                        "author": {"login": "demo_user"},
                                        "body": "demo comments updated!!",
                                    }
                                ],
                            },
                            "labels": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": "abcd4321",
                                },
                                "nodes": [
                                    {
                                        "name": "enhancement",
                                        "description": "New feature or request",
                                    }
                                ],
                            },
                            "assignees": {
                                "pageInfo": {
                                    "hasNextPage": False,
                                    "endCursor": "abcd4321",
                                },
                                "nodes": [{"login": "demo_user"}],
                            },
                        }
                    ]
                }
            }
        }
    }


def attachments():
    return (
        {
            "_id": "demo_repo/source/source.md",
            "_timestamp": "2023-04-17T12:55:01Z",
            "name": "source.md",
            "size": 19,
            "type": "blob",
        },
        {
            "name": "source.md",
            "path": "source/source.md",
            "sha": "c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
            "size": 19,
            "url": "https://api.github.com/repos/demo_user/demo_repo/contents/source/source.md?ref=main",
            "html_url": "https://github.com/demo_user/demo_repo/blob/main/source/source.md",
            "git_url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
            "download_url": "https://raw.githubusercontent.com/demo_user/demo_repo/main/source/source.md",
            "type": "file",
            "content": "VGVzdCBGaWxlICEhISDwn5iCCg==\n",
            "encoding": "base64",
            "_timestamp": "2023-04-17T12:55:01Z",
        },
    )


PUBLIC_REPO = {
    "name": "demo_repo",
    "nameWithOwner": "demo_user/demo_repo",
    "url": "https://github.com/demo_user/demo_repo",
    "description": "Demo repo for poc",
    "visibility": "PUBLIC",
    "primaryLanguage": {"name": "Python"},
    "defaultBranchRef": {"name": "main"},
    "isFork": False,
    "stargazerCount": 0,
    "watchers": {"totalCount": 1},
    "forkCount": 0,
    "createdAt": "2023-04-17T06:06:25Z",
    "_id": "R_kgDOJXuc8A",
    "_timestamp": "2023-06-20T07:09:34Z",
    "isArchived": False,
}
PRIVATE_REPO = {
    "name": "demo_repo",
    "nameWithOwner": "demo_user/demo_repo",
    "url": "https://github.com/demo_user/demo_repo",
    "description": "Demo repo for poc",
    "visibility": "PRIVATE",
    "primaryLanguage": {"name": "Python"},
    "defaultBranchRef": {"name": "main"},
    "isFork": False,
    "stargazerCount": 0,
    "watchers": {"totalCount": 1},
    "forkCount": 0,
    "createdAt": "2023-04-17T06:06:25Z",
    "_id": "R_kgDOJXuc8A",
    "_timestamp": "2023-06-20T07:09:34Z",
    "isArchived": False,
    "type": "Repository",
}
MOCK_RESPONSE_REPO = [
    {
        "user": {
            "repositories": {
                "pageInfo": {"hasNextPage": True, "endCursor": "abcd1234"},
                "nodes": [
                    {
                        "name": "demo_repo",
                        "nameWithOwner": "demo_user/demo_repo",
                    }
                ],
            }
        }
    },
    {
        "user": {
            "repositories": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd4321"},
                "nodes": [
                    {
                        "name": "test_repo",
                        "nameWithOwner": "test_repo",
                    }
                ],
            }
        }
    },
]

MOCK_RESPONSE_ISSUE = {
    "repository": {
        "issues": {
            "nodes": [
                {
                    "number": 1,
                    "url": "https://github.com/demo_user/demo_repo/issues/1",
                    "createdAt": "2023-04-18T10:12:21Z",
                    "closedAt": None,
                    "title": "demo issues",
                    "body": "demo issues test",
                    "state": "OPEN",
                    "id": "I_kwDOJXuc8M5jtMsK",
                    "type": "Issue",
                    "updatedAt": "2023-04-19T08:56:23Z",
                    "author": {
                        "login": "author-mac-author",
                    },
                    "comments": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd4321"},
                        "nodes": [
                            {
                                "author": {"login": "demo_user"},
                                "body": "demo comments updated!!",
                            }
                        ],
                    },
                    "labels": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd4321"},
                        "nodes": [
                            {
                                "name": "enhancement",
                                "description": "New feature or request",
                            }
                        ],
                    },
                    "assignees": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd4321"},
                        "nodes": [{"login": "demo_user"}],
                    },
                }
            ]
        }
    }
}
EXPECTED_ISSUE = {
    "number": 1,
    "url": "https://github.com/demo_user/demo_repo/issues/1",
    "createdAt": "2023-04-18T10:12:21Z",
    "closedAt": None,
    "title": "demo issues",
    "body": "demo issues test",
    "state": "OPEN",
    "type": "Issue",
    "_id": "I_kwDOJXuc8M5jtMsK",
    "_timestamp": "2023-04-19T08:56:23Z",
    "author": {"login": "author-mac-author"},
    "issue_comments": [
        {"author": {"login": "demo_user"}, "body": "demo comments updated!!"}
    ],
    "labels_field": [{"name": "enhancement", "description": "New feature or request"}],
    "assignees_list": [{"login": "demo_user"}],
}
MOCK_RESPONSE_PULL = {
    "repository": {
        "pullRequests": {
            "nodes": [
                {
                    "id": "1",
                    "updatedAt": "2023-07-03T12:24:16Z",
                    "number": 2,
                    "url": "https://github.com/demo_repo/demo_repo/pull/2",
                    "createdAt": "2023-04-19T09:06:01Z",
                    "closedAt": "None",
                    "title": "test pull request",
                    "body": "test pull request",
                    "state": "OPEN",
                    "mergedAt": "None",
                    "author": {"login": "author-authorson"},
                    "assignees": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                        "nodes": [{"login": "test_user"}],
                    },
                    "labels": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                        "nodes": [
                            {
                                "name": "bug",
                                "description": "Something isn't working",
                            },
                        ],
                    },
                    "reviewRequests": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "abcd1234"},
                        "nodes": [{"requestedReviewer": {"login": "test_user"}}],
                    },
                    "comments": {
                        "pageInfo": {
                            "hasNextPage": True,
                            "endCursor": "Y3Vyc29yOnYyOpHOXmz8gA==",
                        },
                        "nodes": [
                            {
                                "author": {"login": "test_user"},
                                "body": "issues comments",
                            },
                        ],
                    },
                    "reviews": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "abcd"},
                        "nodes": [
                            {
                                "author": {"login": "test_user"},
                                "state": "COMMENTED",
                                "body": "add some comments",
                                "comments": {
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": "abcd",
                                    },
                                    "nodes": [{"body": "nice!!!"}],
                                },
                            },
                        ],
                    },
                }
            ]
        }
    }
}
EXPECTED_PULL_RESPONSE = {
    "number": 2,
    "url": "https://github.com/demo_repo/demo_repo/pull/2",
    "createdAt": "2023-04-19T09:06:01Z",
    "closedAt": "None",
    "title": "test pull request",
    "body": "test pull request",
    "state": "OPEN",
    "mergedAt": "None",
    "_id": "1",
    "_timestamp": "2023-07-03T12:24:16Z",
    "type": "Pull request",
    "author": {
        "login": "author-authorson",
    },
    "issue_comments": [
        {"author": {"login": "test_user"}, "body": "issues comments"},
        {"author": {"login": "test_user"}, "body": "more_comments"},
    ],
    "reviews_comments": [
        {
            "author": "test_user",
            "body": "add some comments",
            "state": "COMMENTED",
            "comments": [{"body": "nice!!!"}],
        },
        {
            "author": "test_user",
            "body": "LGTM",
            "state": "APPROVED",
            "comments": [{"body": "LGTM"}],
        },
    ],
    "labels_field": [
        {"name": "bug", "description": "Something isn't working"},
        {"name": "8.8.0", "description": "8.8 Version"},
    ],
    "assignees_list": [{"login": "test_user"}, {"login": "test_user"}],
    "requested_reviewers": [
        {"requestedReviewer": {"login": "test_user"}},
        {"requestedReviewer": {"login": "other_user"}},
    ],
}
MOCK_REVIEWS_RESPONSE = {
    "repository": {
        "pullRequest": {
            "reviews": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
                "nodes": [
                    {
                        "author": {"login": "test_user"},
                        "state": "APPROVED",
                        "body": "LGTM",
                        "comments": {
                            "pageInfo": {
                                "hasNextPage": False,
                                "endCursor": "abcd",
                            },
                            "nodes": [{"body": "LGTM"}],
                        },
                    },
                ],
            }
        }
    }
}
MOCK_REVIEW_REQUESTED_RESPONSE = {
    "repository": {
        "pullRequest": {
            "reviewRequests": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd1234"},
                "nodes": [{"requestedReviewer": {"login": "other_user"}}],
            }
        }
    }
}
MOCK_COMMENTS_RESPONSE = {
    "repository": {
        "pullRequest": {
            "comments": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
                "nodes": [
                    {
                        "author": {"login": "test_user"},
                        "body": "more_comments",
                    },
                ],
            }
        }
    }
}
MOCK_LABELS_RESPONSE = {
    "repository": {
        "pullRequest": {
            "labels": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
                "nodes": [
                    {
                        "name": "8.8.0",
                        "description": "8.8 Version",
                    },
                ],
            }
        }
    }
}
MOCK_ASSIGNEE_RESPONSE = {
    "repository": {
        "pullRequest": {
            "assignees": {
                "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
                "nodes": [
                    {"login": "test_user"},
                ],
            }
        }
    }
}
MOCK_RESPONSE_ATTACHMENTS = (
    {
        "_id": "demo_repo/source/source.md",
        "_timestamp": "2023-04-17T12:55:01Z",
        "name": "source.md",
        "size": 19,
        "type": "blob",
    },
    {
        "name": "source.md",
        "path": "source/source.md",
        "sha": "c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
        "size": 19,
        "url": "https://api.github.com/repos/demo_user/demo_repo/contents/source/source.md?ref=main",
        "html_url": "https://github.com/demo_user/demo_repo/blob/main/source/source.md",
        "git_url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
        "download_url": "https://raw.githubusercontent.com/demo_user/demo_repo/main/source/source.md",
        "type": "file",
        "content": "VGVzdCBGaWxlICEhISDwn5iCCg==\n",
        "encoding": "base64",
        "_timestamp": "2023-04-17T12:55:01Z",
    },
)
MOCK_ATTACHMENT = {
    "path": "source/source.md",
    "mode": "100644",
    "type": "blob",
    "sha": "36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
    "size": 30,
    "url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
    "_timestamp": "2023-04-17T12:55:01Z",
    "repo_name": "demo_repo",
    "name": "source.md",
    "extension": ".md",
}
MOCK_TREE = {
    "sha": "88e3af5daf88e64b273648h37gdf6a561d7092be",
    "url": "https://api.github.com/repos/demo_user/demo_repo/git/trees/88e3af5daf88e64b273648h37gdf6a561d7092be",
    "tree": [
        {
            "path": "source/source.md",
            "mode": "100644",
            "type": "blob",
            "sha": "36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
            "size": 30,
            "url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
        }
    ],
}
MOCK_COMMITS = [
    {
        "sha": "6dcb0c46c273d316e5edc3a6deff2d1bd02",
        "node_id": "C_kwDOJXuc8NoAZ6Y25wY3Q2t2g3N2Q3MTY0ZTVlY2RjM2JhNmQ5ZWY2ZjMWJkMDI",
        "commit": {
            "author": {
                "name": "demo_user",
                "email": "124243629+demo_user@users.noreply.github.com",
                "date": "2023-04-17T12:55:01Z",
            },
            "committer": {
                "name": "GitHub",
                "email": "noreply@github.com",
                "date": "2023-04-17T12:55:01Z",
            },
            "message": "add hello_world.py file",
        },
        "author": {
            "login": "demo_user",
            "id": 128284349,
            "node_id": "U_kgDOB2fOrQ",
            "type": "User",
            "site_admin": False,
        },
        "committer": {
            "login": "web-flow",
            "id": 18766544,
            "node_id": "MD766rNl2jE5ODY0NDQ3",
            "type": "User",
            "site_admin": False,
        },
    }
]
MOCK_RESPONSE_MEMBERS = {
    "organization": {
        "membersWithRole": {
            "edges": [
                {
                    "node": {
                        "id": "#123",
                        "login": "demo-user",
                        "name": "demo",
                        "email": "demo@example.com",
                        "updatedAt": "2023-04-17T12:55:01Z",
                    }
                }
            ]
        }
    }
}
EXPECTED_ACCESS_CONTROL = [
    {
        "_id": "#123",
        "identity": {
            "user_id": "user_id:#123",
            "user_name": "username:demo-user",
            "email": "email:demo@example.com",
        },
        "created_at": "2023-04-17T12:55:01Z",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "user_id:#123",
                        "username:demo-user",
                        "email:demo@example.com",
                    ]
                },
                "source": DLS_QUERY,
            }
        },
    }
]
EXPECTED_ACCESS_CONTROL_GITHUB_APP = [
    {
        "_id": "#1",
        "identity": {
            "user_id": "user_id:#1",
            "user_name": "username:user_1",
            "email": "email:user_1@example.com",
        },
        "created_at": "2023-04-17T12:55:01Z",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "user_id:#1",
                        "username:user_1",
                        "email:user_1@example.com",
                    ]
                },
                "source": DLS_QUERY,
            }
        },
    },
    {
        "_id": "#2",
        "identity": {
            "user_id": "user_id:#2",
            "user_name": "username:user_2",
            "email": "email:user_2@example.com",
        },
        "created_at": "2023-04-17T12:55:01Z",
        "query": {
            "template": {
                "params": {
                    "access_control": [
                        "user_id:#2",
                        "username:user_2",
                        "email:user_2@example.com",
                    ]
                },
                "source": DLS_QUERY,
            }
        },
    },
]
MOCK_CONTRIBUTOR = {
    "repository": {
        "collaborators": {
            "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
            "edges": [
                {
                    "node": {
                        "id": "#123",
                        "login": "demo-user",
                        "email": "demo@email.com",
                    }
                }
            ],
        }
    }
}
MOCK_ORG_REPOS = {
    "organization": {
        "repositories": {
            "pageInfo": {
                "hasNextPage": False,
                "endCursor": "abcd1234",
            },
            "nodes": [
                {
                    "name": "org1",
                    "nameWithOwner": "org1/repo1",
                }
            ],
        }
    }
}
MOCK_INSTALLATIONS = [
    {"id": 1, "account": {"login": "org_1", "type": "Organization"}},
    {"id": 2, "account": {"login": "org_2", "type": "Organization"}},
    {"id": 3, "account": {"login": "user_1", "type": "User"}},
    {"id": 4, "account": {"login": "user_2", "type": "User"}},
]
MOCK_REPO_1 = {
    "nameWithOwner": "org_1/repo_1",
    "id": "repo_1_id",
    "updatedAt": "2023-04-17T12:55:01Z",
}
MOCK_REPO_1_DOC = {
    "nameWithOwner": "org_1/repo_1",
    "_id": "repo_1_id",
    "_timestamp": "2023-04-17T12:55:01Z",
    "type": "Repository",
}
MOCK_REPO_2 = {
    "nameWithOwner": "org_2/repo_2",
    "id": "repo_2_id",
    "updatedAt": "2023-04-17T12:55:01Z",
}
MOCK_REPO_2_DOC = {
    "nameWithOwner": "org_2/repo_2",
    "_id": "repo_2_id",
    "_timestamp": "2023-04-17T12:55:01Z",
    "type": "Repository",
}
MOCK_REPO_3 = {
    "nameWithOwner": "user_1/repo_3",
    "id": "repo_3_id",
    "updatedAt": "2023-04-17T12:55:01Z",
}
MOCK_REPO_3_DOC = {
    "nameWithOwner": "user_1/repo_3",
    "_id": "repo_3_id",
    "_timestamp": "2023-04-17T12:55:01Z",
    "type": "Repository",
}
MOCK_REPO_4 = {
    "nameWithOwner": "user_2/repo_4",
    "id": "repo_4_id",
    "updatedAt": "2023-04-17T12:55:01Z",
}
MOCK_REPO_4_DOC = {
    "nameWithOwner": "user_2/repo_4",
    "_id": "repo_4_id",
    "_timestamp": "2023-04-17T12:55:01Z",
    "type": "Repository",
}


@asynccontextmanager
async def create_github_source(
    auth_method=PERSONAL_ACCESS_TOKEN,
    repo_type="other",
    org_name="",
    repos="*",
    use_document_level_security=False,
    use_text_extraction_service=False,
):
    async with create_source(
        GitHubDataSource,
        data_source="github-server",
        auth_method=auth_method,
        token="changeme",
        app_id=10,
        private_key="changeme",
        repositories=repos,
        repo_type=repo_type,
        org_name=org_name,
        ssl_enabled=False,
        use_document_level_security=use_document_level_security,
        use_text_extraction_service=use_text_extraction_service,
    ) as source:
        yield source


class JSONAsyncMock(AsyncMock):
    def __init__(self, json, status, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json = json
        self.status = status

    async def json(self):
        return self._json


def get_json_mock(mock_response, status):
    async_mock = AsyncMock()
    async_mock.__aenter__ = AsyncMock(
        return_value=JSONAsyncMock(json=mock_response, status=status)
    )
    return async_mock


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["repositories", "token"])
async def test_validate_config_missing_fields_then_raise(field):
    async with create_github_source() as source:
        source.configuration.get_field(field).value = ""

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection():
    async with create_github_source() as source:
        source.github_client._get_client.graphql = AsyncMock(
            return_value={"user": "username"}
        )
        await source.ping()


@pytest.mark.asyncio
async def test_get_user_repos():
    actual_response = []
    async with create_github_source() as source:
        source.github_client.paginated_api_call = Mock(
            return_value=AsyncIterator(MOCK_RESPONSE_REPO)
        )
        async for repo in source.github_client.get_user_repos("demo_repo"):
            actual_response.append(repo)
        assert actual_response == [
            {"name": "demo_repo", "nameWithOwner": "demo_user/demo_repo"},
            {"name": "test_repo", "nameWithOwner": "test_repo"},
        ]


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_ping_with_unsuccessful_connection():
    async with create_github_source() as source:
        with patch.object(
            source.github_client,
            "ping",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scopes",
    [{}, {"repo"}, {"manage_runner:org, delete:packages, admin:public_key"}],
)
async def test_validate_config_with_insufficient_scope(scopes):
    async with create_github_source() as source:
        source.github_client.get_personal_access_token_scopes = AsyncMock(
            return_value=scopes
        )
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Configured token does not have required rights to fetch the content",
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_extra_scopes_token(patch_logger):
    async with create_github_source() as source:
        source.github_client.get_personal_access_token_scopes = AsyncMock(
            return_value={"user", "repo", "admin:org"}
        )
        await source.validate_config()
        patch_logger.assert_present(
            "The provided token has higher privileges than required. It is advisable to run the connector with least privielged token. Required scopes are 'repo', 'user', and 'read:org'."
        )


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_validate_config_with_inaccessible_repositories_then_raise():
    async with create_github_source(
        repos="repo1m owner1/repo1, repo2, owner2/repo2"
    ) as source:
        source.github_client.get_personal_access_token_scopes = AsyncMock(
            return_value={"repo"}
        )
        source.get_invalid_repos = AsyncMock(return_value=["repo2"])
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_invalid_repos_with_max_retries():
    async with create_github_source() as source:
        with pytest.raises(Exception):
            source.github_client.graphql = AsyncMock(side_effect=Exception())
            await source.get_invalid_repos()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_response_with_rate_limit_exceeded():
    async with create_github_source() as source:
        with patch.object(
            source.github_client._get_client,
            "getitem",
            side_effect=ClientResponseError(
                status=403,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
            ),
        ):
            with pytest.raises(Exception):
                source.github_client._get_retry_after = AsyncMock(return_value=0)
                await source.github_client.get_github_item("/user")


@pytest.mark.asyncio
async def test_put_to_sleep():
    async with create_github_source() as source:
        source.github_client._get_retry_after = AsyncMock(return_value=0)
        with pytest.raises(Exception, match="Rate limit exceeded."):
            await source.github_client._put_to_sleep("core")


@pytest.mark.asyncio
async def test_get_retry_after():
    async with create_github_source() as source:
        source.github_client._get_client.getitem = AsyncMock(
            return_value={
                "resources": {
                    "core": {"reset": 1686563525},
                    "graphql": {"reset": 1686563525},
                }
            }
        )
        await source.github_client._get_retry_after(resource_type="core")
        await source.github_client._get_retry_after(resource_type="graphql")


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_graphql_with_errors():
    async with create_github_source() as source:
        source.github_client._get_client.graphql = Mock(
            side_effect=QueryError(
                {"errors": [{"type": "QUERY", "message": "Invalid query"}]}
            )
        )
        with pytest.raises(Exception):
            await source.github_client.graphql(
                {"variable": {"owner": "demo_user"}, "query": "QUERY"}
            )


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_graphql_with_unauthorized():
    async with create_github_source() as source:
        source.github_client._get_client.graphql = Mock(
            side_effect=GraphQLAuthorizationFailure(
                response={"message": "Unauthorized access"}
            )
        )
        with pytest.raises(UnauthorizedException):
            await source.github_client.graphql(
                query="QUERY", variables={"owner": "demo_user"}
            )


@pytest.mark.asyncio
async def test_paginated_api_call():
    expected_response = MOCK_RESPONSE_REPO
    async with create_github_source() as source:
        actual_response = []
        with patch.object(
            source.github_client, "graphql", side_effect=expected_response
        ):
            async for data in source.github_client.paginated_api_call(
                "demo_query", {"owner": "demo_user"}, ["user", "repositories"]
            ):
                actual_response.append(data)
            assert expected_response == actual_response


@pytest.mark.asyncio
async def test_get_invalid_repos():
    expected_response = ["owner1/repo2", "owner2/repo2"]
    async with create_github_source(
        repos="repo1, owner1/repo2, repo2, owner2/repo2"
    ) as source:
        source.github_client.graphql = AsyncMock(
            side_effect=[
                {"viewer": {"login": "owner1"}},
                {
                    "user": {
                        "repositories": {
                            "pageInfo": {
                                "hasNextPage": True,
                                "endCursor": "abcd1234",
                            },
                            "nodes": [
                                {
                                    "name": "owner1",
                                    "nameWithOwner": "owner1/repo2",
                                }
                            ],
                        }
                    }
                },
                Exception(),
            ]
        )
        source.github_client.get_user_repos = Mock(
            return_value=AsyncIterator([{"nameWithOwner": "owner1/repo1"}])
        )
        invalid_repos = await source.get_invalid_repos()
        assert expected_response == invalid_repos


@pytest.mark.asyncio
async def test_get_invalid_repos_organization():
    expected_response = ["owner1/repo2", "org1/repo3"]
    async with create_github_source(
        repos="repo1, owner1/repo2, repo3", repo_type="organization", org_name="org1"
    ) as source:
        source.github_client.graphql = AsyncMock(return_value=MOCK_ORG_REPOS)

        invalid_repos = await source.get_invalid_repos()
        assert sorted(expected_response) == sorted(invalid_repos)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repo_type, configured_repos, expected_invalid_repos",
    [
        (
            "organization",
            "a_repo_without_owner, invalid/repo/format",
            {"a_repo_without_owner", "invalid/repo/format"},
        ),
        (
            "organization",
            "org_1/repo_1, org_2/repo_2, user_1/repo_2, org_1/fake_repo",
            {"user_1/repo_2", "org_1/fake_repo"},
        ),
        (
            "other",
            "user_1/repo_1, user_2/repo_2, org_1/repo_2, user_1/fake_repo",
            {"org_1/repo_2", "user_1/fake_repo"},
        ),
    ],
)
async def test_get_invalid_repos_organization_for_github_app(
    repo_type, configured_repos, expected_invalid_repos
):
    async with create_github_source(
        auth_method=GITHUB_APP, repos=configured_repos, repo_type=repo_type
    ) as source:
        source.github_client.get_installations = Mock(
            return_value=AsyncIterator(MOCK_INSTALLATIONS)
        )
        source.github_client._installation_access_token = "changeme"
        source.github_client._update_installation_access_token = AsyncMock()
        source.github_client.get_org_repos = Mock(
            side_effect=(
                lambda owner: AsyncIterator(
                    [
                        {"nameWithOwner": "org_1/repo_1"},
                        {"nameWithOwner": "org_1/repo_2"},
                    ]
                )
                if owner == "org_1"
                else AsyncIterator(
                    [
                        {"nameWithOwner": "org_2/repo_1"},
                        {"nameWithOwner": "org_2/repo_2"},
                    ]
                )
            )
        )
        source.github_client.get_user_repos = Mock(
            side_effect=(
                lambda owner: AsyncIterator(
                    [
                        {"nameWithOwner": "user_1/repo_1"},
                        {"nameWithOwner": "user_1/repo_2"},
                    ]
                )
                if owner == "user_1"
                else AsyncIterator(
                    [
                        {"nameWithOwner": "user_2/repo_2"},
                        {"nameWithOwner": "user_2/repo_2"},
                    ]
                )
            )
        )

        invalid_repos = await source.get_invalid_repos()
        assert set(invalid_repos) == expected_invalid_repos


@pytest.mark.asyncio
async def test_get_content_with_md_file():
    expected_response = {
        "_id": "demo_repo/source.md",
        "_timestamp": "2023-04-17T12:55:01Z",
        "_attachment": "VGVzdCBGaWxlICEhISDwn5iCCg==",
    }
    async with create_github_source() as source:
        with patch.object(
            source.github_client._get_client,
            "getitem",
            side_effect=[MOCK_RESPONSE_ATTACHMENTS[1]],
        ):
            actual_response = await source.get_content(
                attachment=MOCK_ATTACHMENT, doit=True
            )
            assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_content_with_md_file_with_extraction_service():
    with patch(
        "connectors.content_extraction.ContentExtraction.extract_text",
        return_value="Test File !!! U+1F602",
    ), patch(
        "connectors.content_extraction.ContentExtraction.get_extraction_config",
        return_value={"host": "http://localhost:8090"},
    ):
        expected_response = {
            "_id": "demo_repo/source.md",
            "_timestamp": "2023-04-17T12:55:01Z",
            "body": "Test File !!! U+1F602",
        }
        async with create_github_source(use_text_extraction_service=True) as source:
            with patch.object(
                source.github_client._get_client,
                "getitem",
                side_effect=[MOCK_RESPONSE_ATTACHMENTS[1]],
            ):
                actual_response = await source.get_content(
                    attachment=MOCK_ATTACHMENT, doit=True
                )
                assert actual_response == expected_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size, expected_content",
    [
        (0, None),
        (23000000, None),
    ],
)
async def test_get_content_with_differernt_size(size, expected_content):
    async with create_github_source() as source:
        attachment_with_size_zero = MOCK_ATTACHMENT.copy()
        attachment_with_size_zero["size"] = size
        response = await source.get_content(
            attachment=attachment_with_size_zero, doit=True
        )
        assert response is expected_content


@pytest.mark.asyncio
async def test_fetch_repos():
    async with create_github_source() as source:
        source.github_client.graphql = AsyncMock(
            return_value={"data": {"viewer": {"login": "owner1"}}}
        )
        source.github_client.get_user_repos = Mock(
            return_value=AsyncIterator(
                [
                    {
                        "id": "123",
                        "updatedAt": "2023-04-17T12:55:01Z",
                        "nameWithOwner": "owner1/repo1",
                    }
                ]
            )
        )
        async for repo in source._fetch_repos():
            assert repo == {
                "nameWithOwner": "owner1/repo1",
                "_id": "123",
                "_timestamp": "2023-04-17T12:55:01Z",
                "type": "Repository",
            }


@pytest.mark.asyncio
async def test_fetch_repos_organization():
    async with create_github_source(
        repo_type="organization", org_name="org_1"
    ) as source:
        source.github_client.graphql = AsyncMock(
            return_value={"data": {"viewer": {"login": "owner1"}}}
        )
        source.github_client.get_org_repos = Mock(
            side_effect=AsyncIterator([MOCK_REPO_1])
        )
        async for repo in source._fetch_repos():
            assert repo == MOCK_REPO_1_DOC


@pytest.mark.asyncio
async def test_fetch_repos_when_user_repos_is_available():
    async with create_github_source(repos="demo_user/demo_repo, , demo_repo") as source:
        source.github_client.graphql = AsyncMock(
            side_effect=[
                {"viewer": {"login": "owner1"}},
                {
                    "repository": {
                        "id": "123",
                        "updatedAt": "2023-04-17T12:55:01Z",
                        "nameWithOwner": "demo_user/demo_repo",
                    }
                },
            ]
        )
        async for repo in source._fetch_repos():
            assert repo == {
                "nameWithOwner": "demo_user/demo_repo",
                "_id": "123",
                "_timestamp": "2023-04-17T12:55:01Z",
                "type": "Repository",
            }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [UnauthorizedException, ForbiddenException],
)
async def test_fetch_repos_with_client_exception(exception):
    async with create_github_source() as source:
        source.github_client.graphql = Mock(side_effect=exception())
        with pytest.raises(exception):
            async for _ in source._fetch_repos():
                pass


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repo_type, repos, expected_repos",
    [
        ("organization", "*", [MOCK_REPO_1_DOC, MOCK_REPO_2_DOC]),
        ("other", "*", [MOCK_REPO_3_DOC, MOCK_REPO_4_DOC]),
        ("organization", "org_1/repo_1", [MOCK_REPO_1_DOC]),
        (
            "organization",
            "org_1/repo_1, org_2/repo_2",
            [MOCK_REPO_1_DOC, MOCK_REPO_2_DOC],
        ),
        ("other", "user_1/repo_3", [MOCK_REPO_3_DOC]),
        ("other", "user_1/repo_3, user_2/repo_4", [MOCK_REPO_3_DOC, MOCK_REPO_4_DOC]),
    ],
)
async def test_fetch_repos_github_app(repo_type, repos, expected_repos):
    async with create_github_source(
        auth_method=GITHUB_APP, repo_type=repo_type, repos=repos
    ) as source:
        source.github_client.get_installations = Mock(
            return_value=AsyncIterator(MOCK_INSTALLATIONS)
        )
        source.github_client.get_user_repos = Mock(
            side_effect=lambda user: AsyncIterator([MOCK_REPO_3])
            if user == "user_1"
            else AsyncIterator([MOCK_REPO_4])
        )
        source.github_client.get_org_repos = Mock(
            side_effect=lambda org_name: AsyncIterator([MOCK_REPO_1])
            if org_name == "org_1"
            else AsyncIterator([MOCK_REPO_2])
        )
        jwt_response = {"token": "changeme"}
        with patch(
            "connectors.sources.github.get_installation_access_token",
            return_value=jwt_response,
        ):
            actual_repos = [repo async for repo in source._fetch_repos()]
            assert actual_repos == expected_repos


@pytest.mark.asyncio
async def test_fetch_issues():
    async with create_github_source() as source:
        source.fetch_extra_fields = AsyncMock()
        with patch.object(
            source.github_client,
            "paginated_api_call",
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_ISSUE]),
            ],
        ):
            async for issue in source._fetch_issues(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "issues"],
            ):
                assert issue == EXPECTED_ISSUE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [UnauthorizedException, ForbiddenException],
)
async def test_fetch_issues_with_client_exception(exception):
    async with create_github_source() as source:
        source.github_client.graphql = Mock(side_effect=exception())
        with pytest.raises(exception):
            async for _ in source._fetch_issues(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "issues"],
            ):
                pass


@pytest.mark.asyncio
async def test_fetch_pull_requests():
    async with create_github_source() as source:
        with patch.object(
            source.github_client,
            "paginated_api_call",
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_PULL]),
                AsyncIterator([MOCK_COMMENTS_RESPONSE]),
                AsyncIterator([MOCK_REVIEW_REQUESTED_RESPONSE]),
                AsyncIterator([MOCK_LABELS_RESPONSE]),
                AsyncIterator([MOCK_ASSIGNEE_RESPONSE]),
                AsyncIterator([MOCK_REVIEWS_RESPONSE]),
            ],
        ):
            async for pull in source._fetch_pull_requests(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "pullRequests"],
            ):
                assert pull == EXPECTED_PULL_RESPONSE


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [UnauthorizedException, ForbiddenException],
)
async def test_fetch_pull_requests_with_client_exception(exception):
    async with create_github_source() as source:
        source.github_client.graphql = Mock(side_effect=exception())
        with pytest.raises(exception):
            async for _ in source._fetch_pull_requests(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "pullRequests"],
            ):
                pass


@pytest.mark.asyncio
async def test_fetch_pull_requests_with_deleted_users():
    async with create_github_source() as source:
        mock_review_deleted_user = {
            "repository": {
                "pullRequest": {
                    "reviews": {
                        "pageInfo": {"hasNextPage": False, "endCursor": "abcd"},
                        "nodes": [
                            {
                                "author": None,  # author will return None in this situation
                                "state": "APPROVED",
                                "body": "LGTM",
                                "comments": {
                                    "pageInfo": {
                                        "hasNextPage": False,
                                        "endCursor": "abcd",
                                    },
                                    "nodes": [{"body": "LGTM"}],
                                },
                            },
                        ],
                    }
                }
            }
        }

        expected_pull_response_deleted_user = deepcopy(EXPECTED_PULL_RESPONSE)
        expected_pull_response_deleted_user["reviews_comments"] = [
            {
                "author": "test_user",
                "body": "add some comments",
                "state": "COMMENTED",
                "comments": [{"body": "nice!!!"}],
            },
            {
                "author": None,  # deleted author
                "body": "LGTM",
                "state": "APPROVED",
                "comments": [{"body": "LGTM"}],
            },
        ]

        with patch.object(
            source.github_client,
            "paginated_api_call",
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_PULL]),
                AsyncIterator([MOCK_COMMENTS_RESPONSE]),
                AsyncIterator([MOCK_REVIEW_REQUESTED_RESPONSE]),
                AsyncIterator([MOCK_LABELS_RESPONSE]),
                AsyncIterator([MOCK_ASSIGNEE_RESPONSE]),
                AsyncIterator([mock_review_deleted_user]),
            ],
        ):
            async for pull in source._fetch_pull_requests(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "pullRequests"],
            ):
                assert pull == expected_pull_response_deleted_user


@pytest.mark.asyncio
async def test_fetch_files():
    expected_response = (
        {
            "name": "source.md",
            "size": 30,
            "type": "blob",
            "path": "source/source.md",
            "mode": "100644",
            "extension": ".md",
            "_timestamp": "2023-04-17T12:55:01Z",
            "_id": "demo_repo/source/source.md",
        },
        {
            "path": "source/source.md",
            "mode": "100644",
            "type": "blob",
            "sha": "36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
            "size": 30,
            "url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
            "_timestamp": "2023-04-17T12:55:01Z",
            "repo_name": "demo_repo",
            "name": "source.md",
            "extension": ".md",
        },
    )
    async with create_github_source() as source:
        with patch.object(
            source.github_client,
            "get_github_item",
            side_effect=[MOCK_TREE, MOCK_COMMITS],
        ):
            async for document in source._fetch_files("demo_repo", "main"):
                assert expected_response == document


@pytest.mark.asyncio
async def test_get_docs():
    expected_response = [
        PUBLIC_REPO,
        MOCK_RESPONSE_PULL,
        MOCK_RESPONSE_ISSUE,
        MOCK_RESPONSE_ATTACHMENTS[0],
    ]
    actual_response = []
    async with create_github_source() as source:
        source._fetch_repos = Mock(return_value=AsyncIterator([deepcopy(PUBLIC_REPO)]))
        source._fetch_issues = Mock(
            return_value=AsyncIterator([deepcopy(MOCK_RESPONSE_ISSUE)])
        )
        source._fetch_pull_requests = Mock(
            return_value=AsyncIterator([deepcopy(MOCK_RESPONSE_PULL)])
        )
        source._fetch_files = Mock(
            return_value=AsyncIterator([deepcopy(MOCK_RESPONSE_ATTACHMENTS)])
        )
        async for document, _ in source.get_docs():
            actual_response.append(document)
        assert expected_response == actual_response


@pytest.mark.asyncio
async def test_get_docs_with_access_control_should_not_add_acl_for_public_repo():
    public_repo_ = public_repo()
    pull_request_ = pull_request()
    issue_ = issue()
    attachments_ = attachments()

    expected_response = [
        public_repo_,
        pull_request_,
        issue_,
        attachments_[0],
    ]
    actual_response = []
    async with create_github_source() as source:
        source._dls_enabled = Mock(return_value=True)
        source._fetch_repos = Mock(return_value=AsyncIterator([public_repo_]))
        source._fetch_issues = Mock(return_value=AsyncIterator([issue_]))
        source._fetch_pull_requests = Mock(return_value=AsyncIterator([pull_request_]))
        source._fetch_files = Mock(return_value=AsyncIterator([attachments_]))
        async for document, _ in source.get_docs():
            actual_response.append(document)
            assert "_allow_access_control" not in document

        assert expected_response == actual_response


@pytest.mark.asyncio
async def test_get_docs_with_access_control_should_add_acl_for_non_public_repo():
    expected_response = [
        PRIVATE_REPO,
        MOCK_RESPONSE_PULL,
        MOCK_RESPONSE_ISSUE,
        MOCK_RESPONSE_ATTACHMENTS[0],
    ]
    actual_response = []

    async with create_github_source() as source:
        source.github_client.paginated_api_call = Mock(
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_MEMBERS]),
                AsyncIterator([MOCK_CONTRIBUTOR]),
            ]
        )
        source._dls_enabled = Mock(return_value=True)
        source._fetch_repos = Mock(return_value=AsyncIterator([PRIVATE_REPO]))
        source._fetch_issues = Mock(return_value=AsyncIterator([MOCK_RESPONSE_ISSUE]))
        source._fetch_pull_requests = Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_PULL])
        )
        source._fetch_files = Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_ATTACHMENTS])
        )
        async for document, _ in source.get_docs():
            actual_response.append(document)
            assert "_allow_access_control" in document

        assert expected_response == actual_response


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # valid: empty array should be valid
            [],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: empty object should also be valid -> default value in Kibana
            {},
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: valid queries
            [
                {
                    "repository": "repo_name",
                    "filter": {"issue": "is:open", "pr": "is:open", "branch": "main"},
                }
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # valid: optional pr key
            [
                {
                    "repository": "repo_name",
                    "filter": {"issue": "is:open", "branch": "main"},
                }
            ],
            SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            ),
        ),
        (
            # invalid: repository key missing
            [{"filter": {"issue": "is:open", "pr": "is:open", "branch": "main"}}],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: invalid key
            [
                {
                    "repository": "repo_name",
                    "filters": {"issue": "is:open", "pr": "is:open", "branch": "main"},
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: invalid array value
            [
                {
                    "repository": "repo_name",
                    "filters": [
                        {"issue": "is:open", "pr": "is:open", "branch": "main"}
                    ],
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
        (
            # invalid: repository can not be empty
            [
                {
                    "repository": "",
                    "filters": [
                        {"issue": "is:open", "pr": "is:open", "branch": "main"}
                    ],
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation(advanced_rules, expected_validation_result):
    async with create_github_source() as source:
        source.get_invalid_repos = AsyncMock(return_value=[])

        validation_result = await GitHubAdvancedRulesValidator(source).validate(
            advanced_rules
        )

        assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "advanced_rules, expected_validation_result",
    [
        (
            # invalid: invalid repos
            [
                {
                    "repository": "repo_name",
                    "filter": {"issue": "is:open", "pr": "is:open", "branch": "main"},
                }
            ],
            SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=ANY,
            ),
        )
    ],
)
@pytest.mark.asyncio
async def test_advanced_rules_validation_with_invalid_repos(
    advanced_rules, expected_validation_result
):
    async with create_github_source() as source:
        source.get_invalid_repos = AsyncMock(return_value=["repo_name"])

        validation_result = await GitHubAdvancedRulesValidator(source).validate(
            advanced_rules
        )

        assert validation_result == expected_validation_result


@pytest.mark.parametrize(
    "filtering, expected_response",
    [
        (
            # Configured valid queries, without branch
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {
                                "repository": "demo_repo",
                                "filter": {
                                    "issue": "is:open",
                                    "pr": "is:open",
                                    "branch": "main",
                                },
                            },
                        ]
                    }
                }
            ),
            [
                PUBLIC_REPO,
                MOCK_RESPONSE_PULL,
                MOCK_RESPONSE_ISSUE,
                MOCK_RESPONSE_ATTACHMENTS[0],
            ],
        ),
        (
            # Configured invalid queries, with branch
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {
                                "repository": "demo_repo",
                                "filter": {
                                    "issue": "is:pr is:open",
                                    "pr": "is:issue is:open",
                                    "branch": "main",
                                },
                            },
                        ]
                    }
                }
            ),
            [PUBLIC_REPO, MOCK_RESPONSE_ATTACHMENTS[0]],
        ),
        (
            # Configured only branch, without queries
            Filter(
                {
                    ADVANCED_SNIPPET: {
                        "value": [
                            {
                                "repository": "demo_repo",
                                "filter": {
                                    "branch": "main",
                                },
                            },
                        ]
                    }
                }
            ),
            [PUBLIC_REPO, MOCK_RESPONSE_ATTACHMENTS[0]],
        ),
    ],
)
@pytest.mark.asyncio
async def test_get_docs_with_advanced_rules(filtering, expected_response):
    actual_response = []
    async with create_github_source() as source:
        source._get_configured_repos = Mock(return_value=AsyncIterator([PUBLIC_REPO]))
        source._fetch_issues = Mock(return_value=AsyncIterator([MOCK_RESPONSE_ISSUE]))
        source._fetch_pull_requests = Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_PULL])
        )
        source._fetch_files = Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_ATTACHMENTS])
        )
        async for document, _ in source.get_docs(filtering=filtering):
            actual_response.append(document)
        assert expected_response == actual_response


@pytest.mark.asyncio
async def test_is_previous_repo():
    async with create_github_source() as source:
        assert source.is_previous_repo("demo_user/demo_repo") is False
        assert source.is_previous_repo("demo_user/demo_repo") is True


@pytest.mark.asyncio
async def test_get_access_control():
    async with create_github_source(repo_type="organization") as source:
        actual_response = []
        source._dls_enabled = Mock(return_value=True)
        with patch.object(
            source.github_client,
            "paginated_api_call",
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_MEMBERS]),
            ],
        ):
            async for access_control in source.get_access_control():
                actual_response.append(access_control)
        assert actual_response == EXPECTED_ACCESS_CONTROL

        source._dls_enabled = Mock(return_value=False)
        async for access_control in source.get_access_control():
            assert access_control is None


@pytest.mark.asyncio
async def test_get_access_control_github_app():
    async with create_github_source(
        auth_method=GITHUB_APP, repo_type="organization"
    ) as source:
        source._dls_enabled = Mock(return_value=True)
        source.github_client.get_installations = Mock(
            return_value=AsyncIterator(MOCK_INSTALLATIONS)
        )
        source.github_client._installation_access_token = "changeme"
        source.github_client._fetch_all_members = Mock(
            side_effect=(
                lambda org_name: AsyncIterator(
                    [
                        {
                            "id": "#1",
                            "login": "user_1",
                            "email": "user_1@example.com",
                            "updatedAt": "2023-04-17T12:55:01Z",
                        },
                    ]
                )
                if org_name == "org_1"
                else AsyncIterator(
                    [
                        {
                            "id": "#2",
                            "login": "user_2",
                            "email": "user_2@example.com",
                            "updatedAt": "2023-04-17T12:55:01Z",
                        },
                    ]
                )
            )
        )

        actual_response = []
        jwt_response = {"token": "changeme"}
        with patch(
            "connectors.sources.github.get_installation_access_token",
            return_value=jwt_response,
        ):
            async for access_control in source.get_access_control():
                actual_response.append(access_control)
        assert actual_response == EXPECTED_ACCESS_CONTROL_GITHUB_APP


@pytest.mark.asyncio
async def test_fetch_access_control():
    async with create_github_source() as source:
        source.github_client.paginated_api_call = Mock(
            side_effect=[
                AsyncIterator([MOCK_RESPONSE_MEMBERS]),
                AsyncIterator([MOCK_CONTRIBUTOR]),
            ]
        )
        access_control = await source._fetch_access_control("org1/repo1")
        assert sorted(access_control) == sorted(
            ["user_id:#123", "username:demo-user", "email:demo@email.com"]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repo_type, use_document_level_security, dls_enabled",
    [
        ("organization", False, False),
        ("organization", True, True),
        ("other", False, False),
        ("other", True, False),
    ],
)
async def test_dls_enabled(repo_type, use_document_level_security, dls_enabled):
    async with create_github_source(
        repo_type=repo_type, use_document_level_security=use_document_level_security
    ) as source:
        source.set_features(Features(GitHubDataSource.features()))
        assert source._dls_enabled() == dls_enabled


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scopes, expected_scopes",
    [
        (None, set()),
        ("", set()),
        ("repo", {"repo"}),
        ("repo, read:org", {"repo", "read:org"}),
    ],
)
async def test_get_personal_access_token_scopes(scopes, expected_scopes):
    async with create_github_source() as source:
        source.github_client._get_client._request = AsyncMock(
            return_value=(200, {"X-OAuth-Scopes": scopes}, None)
        )
        actual_scopes = await source.github_client.get_personal_access_token_scopes()
        assert actual_scopes == expected_scopes


@pytest.mark.asyncio
async def test_github_client_get_installations():
    async with create_github_source(auth_method=GITHUB_APP) as source:
        mock_response = [
            {
                "id": "expected_installation",
                "suspended_at": None,
            },
            {
                "id": "filtered_installation",
                "suspended_at": "2017-07-08T16:18:44-04:00",
            },
        ]
        source.github_client._get_client._make_request = AsyncMock(
            return_value=(mock_response, None)
        )
        with patch("connectors.sources.github.get_jwt", return_value="changeme"):
            expected_installations = [
                installation
                async for installation in source.github_client.get_installations()
            ]
            source.github_client._get_client._make_request.assert_awaited_with(
                "GET", "/app/installations", ANY, ANY, ANY, ANY
            )
            assert len(expected_installations) == 1
            assert expected_installations[0]["id"] == "expected_installation"


@pytest.mark.asyncio
async def test_github_app_paginated_get():
    async with create_github_source(auth_method=GITHUB_APP) as source:
        item_1 = {"id": 1}
        item_2 = {"id": 2}
        item_3 = {"id": 3}
        source.github_client._get_client._make_request = AsyncMock(
            side_effect=[([item_1, item_2], "fake_url_2"), ([item_3], None)]
        )
        with patch("connectors.sources.github.get_jwt", return_value="changeme"):
            expected_results = [
                item
                async for item in source.github_client._github_app_paginated_get(
                    "fake_url_1"
                )
            ]
            assert source.github_client._get_client._make_request.await_count == 2
            assert len(expected_results) == 3
            assert expected_results == [item_1, item_2, item_3]


@pytest.mark.asyncio
async def test_update_installation_id():
    async with create_github_source(auth_method=GITHUB_APP) as source:
        jwt_response = {"token": "changeme"}
        installation_id = 123
        with patch(
            "connectors.sources.github.get_installation_access_token",
            return_value=jwt_response,
        ) as get_installation_access_token:
            assert source.github_client._installation_id is None
            assert source.github_client._installation_access_token is None
            await source.github_client.update_installation_id(installation_id)
            assert source.github_client._installation_id == installation_id
            assert source.github_client._installation_access_token == "changeme"
            get_installation_access_token.assert_awaited_with(
                gh=ANY, installation_id=installation_id, app_id=ANY, private_key=ANY
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_method, expected_await_count, expected_user",
    [
        (GITHUB_APP, 0, None),
        (PERSONAL_ACCESS_TOKEN, 1, "foo"),
    ],
)
async def test_logged_in_user(auth_method, expected_await_count, expected_user):
    async with create_github_source(auth_method=auth_method) as source:
        source.github_client.get_logged_in_user = AsyncMock(return_value="foo")
        user = await source._logged_in_user()
        assert user == expected_user
        user = await source._logged_in_user()
        assert user == expected_user
        assert (
            source.github_client.get_logged_in_user.await_count == expected_await_count
        )


@pytest.mark.asyncio
async def test_fetch_installations_personal_access_token():
    async with create_github_source() as source:
        source.github_client.get_installations = AsyncMock()
        await source._fetch_installations()
        assert len(source._installations) == 0
        source.github_client.get_installations.assert_not_called()


@pytest.mark.asyncio
async def test_fetch_installations_withp_prepopulated_installations():
    prepopulated_installations = {"fake_org": {"installation_id": 1}}
    async with create_github_source(auth_method=GITHUB_APP) as source:
        source.github_client.get_installations = AsyncMock()
        source._installations = prepopulated_installations
        await source._fetch_installations()
        assert source._installations == prepopulated_installations
        source.github_client.get_installations.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repo_type, expected_installations",
    [
        (
            "organization",
            {"org_1": 1, "org_2": 2},
        ),
        ("other", {"user_1": 3, "user_2": 4}),
    ],
)
async def test_fetch_installations(repo_type, expected_installations):
    async with create_github_source(
        auth_method=GITHUB_APP, repo_type=repo_type
    ) as source:
        source.github_client.get_installations = Mock(
            return_value=AsyncIterator(MOCK_INSTALLATIONS)
        )
        await source._fetch_installations()
        assert source._installations == expected_installations
        source.github_client.get_installations.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "auth_method, repo_type, expected_owners",
    [
        (PERSONAL_ACCESS_TOKEN, "organization", ["demo_org"]),
        (PERSONAL_ACCESS_TOKEN, "other", ["demo_user"]),
        (GITHUB_APP, "organization", ["org_1", "org_2"]),
        (GITHUB_APP, "other", ["user_1", "user_2"]),
    ],
)
async def test_get_owners(auth_method, repo_type, expected_owners):
    async with create_github_source(
        auth_method=auth_method, repo_type=repo_type, org_name="demo_org"
    ) as source:
        source.github_client.get_logged_in_user = AsyncMock(return_value="demo_user")
        source.github_client.get_installations = Mock(
            return_value=AsyncIterator(MOCK_INSTALLATIONS)
        )
        jwt_response = {"token": "changeme"}
        with patch(
            "connectors.sources.github.get_installation_access_token",
            return_value=jwt_response,
        ):
            actual_owners = [owner async for owner in source._get_owners()]
            assert actual_owners == expected_owners
