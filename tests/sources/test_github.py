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

from connectors.access_control import DLS_QUERY
from connectors.filtering.validation import SyncRuleValidationResult
from connectors.protocol import Features, Filter
from connectors.source import ConfigurableFieldValueError
from connectors.sources.github import (
    REPOSITORY_OBJECT,
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
    "type": "Repository",
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
        "data": {
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
        }
    },
    {
        "data": {
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
        }
    },
]

MOCK_RESPONSE_ISSUE = {
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
    "issue_comments": [
        {"author": {"login": "demo_user"}, "body": "demo comments updated!!"}
    ],
    "labels_field": [{"name": "enhancement", "description": "New feature or request"}],
    "assignees_list": [{"login": "demo_user"}],
}
MOCK_RESPONSE_PULL = {
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
    "data": {
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
}
MOCK_REVIEW_REQUESTED_RESPONSE = {
    "data": {
        "repository": {
            "pullRequest": {
                "reviewRequests": {
                    "pageInfo": {"hasNextPage": False, "endCursor": "abcd1234"},
                    "nodes": [{"requestedReviewer": {"login": "other_user"}}],
                }
            }
        }
    }
}
MOCK_COMMENTS_RESPONSE = {
    "data": {
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
}
MOCK_LABELS_RESPONSE = {
    "data": {
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
}
MOCK_ASSIGNEE_RESPONSE = {
    "data": {
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
    "data": {
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
MOCK_CONTRIBUTOR = {
    "data": {
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
}
MOCK_ORG_REPOS = {
    "data": {
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
}


@asynccontextmanager
async def create_github_source(
    repo_type="other",
    org_name="",
    repos="*",
    use_document_level_security=False,
    use_text_extraction_service=False,
):
    async with create_source(
        GitHubDataSource,
        data_source="github-server",
        token="changeme",
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
        source.github_client._get_session.post = Mock(
            return_value=get_json_mock(mock_response={"user": "username"}, status=200)
        )
        await source.ping()


@pytest.mark.asyncio
async def test_get_user_repos():
    actual_response = []
    async with create_github_source() as source:
        source.github_client.paginated_api_call = Mock(
            return_value=AsyncIterator(MOCK_RESPONSE_REPO)
        )
        async for repo in source.github_client.get_user_repos("demo_user"):
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
async def test_validate_config_with_invalid_token_then_raise():
    async with create_github_source() as source:
        with patch.object(
            source.github_client,
            "post",
            side_effect=UnauthorizedException(),
        ):
            with pytest.raises(UnauthorizedException):
                await source.validate_config()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scopes",
    ["", "repo", "manage_runner:org, delete:packages, admin:public_key"],
)
async def test_validate_config_with_insufficient_scope(scopes):
    async with create_github_source() as source:
        source.github_client.post = AsyncMock(
            return_value=({"user": "username"}, {"X-OAuth-Scopes": scopes})
        )
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Configured token does not have required rights to fetch the content",
        ):
            await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_extra_scopes_token(patch_logger):
    async with create_github_source() as source:
        source.github_client.post = AsyncMock(
            return_value=(
                {"user": "username"},
                {"X-OAuth-Scopes": "user, repo, admin:org"},
            )
        )
        await source.validate_config()
        patch_logger.assert_present(
            "The provided token has higher privileges than required. It is advisable to run the connector with least privielged token. Required scopes are 'repo', 'user', and 'read:org'."
        )


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_validate_config_with_inaccessible_repositories_then_raise():
    async with create_github_source(
        repos="repo1, owner1/repo1, repo2, owner2/repo2"
    ) as source:
        source.github_client.post = AsyncMock(
            return_value=({"dummy": "dummy"}, {"X-OAuth-Scopes": "repo"})
        )
        source.get_invalid_repos = AsyncMock(return_value=["repo2"])
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_get_invalid_repos_with_max_retries():
    async with create_github_source() as source:
        with pytest.raises(Exception):
            source.github_client.post = AsyncMock(side_effect=Exception())
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
async def test_post_with_errors():
    async with create_github_source() as source:
        source.github_client._get_session.post = Mock(
            return_value=get_json_mock(
                mock_response={
                    "errors": [{"type": "QUERY", "message": "Invalid query"}]
                },
                status=200,
            )
        )
        with pytest.raises(Exception):
            await source.github_client.post(
                {"variable": {"owner": "demo_user"}, "query": "QUERY"}
            )


@pytest.mark.asyncio
@patch("connectors.utils.time_to_sleep_between_retries", Mock(return_value=0))
async def test_post_with_unauthorized():
    async with create_github_source() as source:
        source.github_client._get_session.post = Mock(
            side_effect=ClientResponseError(
                status=401,
                request_info=aiohttp.RequestInfo(
                    real_url="", method=None, headers=None, url=""
                ),
                history=None,
            )
        )
        with pytest.raises(UnauthorizedException):
            await source.github_client.post(
                {"variable": {"owner": "demo_user"}, "query": "QUERY"}
            )


@pytest.mark.asyncio
async def test_paginated_api_call():
    expected_response = MOCK_RESPONSE_REPO
    async with create_github_source() as source:
        actual_response = []
        with patch.object(source.github_client, "post", side_effect=expected_response):
            async for data in source.github_client.paginated_api_call(
                {"owner": "demo_user"}, "demo_query", ["user", "repositories"]
            ):
                actual_response.append(data)
            assert expected_response == actual_response


@pytest.mark.asyncio
async def test_get_invalid_repos():
    expected_response = ["owner1/repo2", "owner2/repo2"]
    async with create_github_source(
        repos="repo1, owner1/repo2, repo2, owner2/repo2"
    ) as source:
        source.github_client.post = AsyncMock(
            side_effect=[
                {"data": {"viewer": {"login": "owner1"}}},
                {
                    "data": {
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
        repo_type="organization", org_name="org1", repos="repo1, owner1/repo2, repo3"
    ) as source:
        source.github_client.post = AsyncMock(return_value=MOCK_ORG_REPOS)

        invalid_repos = await source.get_invalid_repos()
        assert sorted(expected_response) == sorted(invalid_repos)


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
        source.github_client.post = AsyncMock(
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
        repo_type="organization", org_name="org1"
    ) as source:
        source.github_client.post = AsyncMock(
            return_value={"data": {"viewer": {"login": "owner1"}}}
        )
        source.org_repos = {
            "org1/repo1": {
                "id": "123",
                "nameWithOwner": "org1/repo1",
                "updatedAt": "2023-04-17T12:55:01Z",
            }
        }
        async for repo in source._fetch_repos():
            assert repo == {
                "nameWithOwner": "org1/repo1",
                "_id": "123",
                "_timestamp": "2023-04-17T12:55:01Z",
                "type": "Repository",
            }


@pytest.mark.asyncio
async def test_fetch_repos_when_user_repos_is_available():
    async with create_github_source(repos="demo_user/demo_repo, , demo_repo") as source:
        source.github_client.post = AsyncMock(
            side_effect=[
                {"data": {"viewer": {"login": "owner1"}}},
                {
                    "data": {
                        "repository": {
                            "id": "123",
                            "updatedAt": "2023-04-17T12:55:01Z",
                            "nameWithOwner": "demo_user/demo_repo",
                        }
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
async def test_fetch_repos_with_unauthorized_exception():
    async with create_github_source() as source:
        source.github_client.post = Mock(side_effect=UnauthorizedException())
        with pytest.raises(UnauthorizedException):
            async for _ in source._fetch_repos():
                pass


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
async def test_fetch_issues_with_unauthorized_exception():
    async with create_github_source() as source:
        source.github_client.post = Mock(side_effect=UnauthorizedException())
        with pytest.raises(UnauthorizedException):
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
async def test_fetch_pull_requests_with_unauthorized_exception():
    async with create_github_source() as source:
        source.github_client.post = Mock(side_effect=UnauthorizedException())
        with pytest.raises(UnauthorizedException):
            async for _ in source._fetch_pull_requests(
                repo_name="demo_user/demo_repo",
                response_key=[REPOSITORY_OBJECT, "pullRequests"],
            ):
                pass


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
    async with create_github_source() as source:
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
