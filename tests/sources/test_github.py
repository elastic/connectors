#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Tests the Github source class methods"""
from http import HTTPStatus
from unittest import mock

import pytest
from gidgethub import BadRequest

from connectors.source import ConfigurableFieldValueError, DataSourceConfiguration
from connectors.sources import github
from connectors.sources.github import GitHubDataSource, UnauthorizedException
from tests.commons import AsyncIterator
from tests.sources.support import AsyncSourceContextManager, create_source

MOCK_RESPONSE_REPO = {
    "_id": "625848393",
    "_timestamp": "2023-04-19T09:32:54Z",
    "name": "demo_repo",
    "full_name": "demo_user/demo_repo",
    "html_url": "https://github.com/demo_user/demo_repo",
    "owner": "demo_user",
    "type": "Repository",
    "description": "this is a test repository",
    "visibility": "public",
    "language": "Python",
    "default_branch": "main",
    "fork": "false",
    "open_issues": 4,
    "forks_count": 2,
    "watchers": 10,
    "stargazers_count": 10,
    "created_at": "2023-04-19T09:32:54Z",
}

MOCK_RESPONSE_ISSUE = {
    "_id": "1672779543",
    "_timestamp": "2023-04-20T08:56:23Z",
    "number": 1,
    "html_url": "https://github.com/demo_user/demo_repo/issues/1",
    "created_at": "2023-04-19T08:56:23Z",
    "closed_at": "2023-04-20T08:56:23Z",
    "title": "demo issues",
    "body": "demo issues test",
    "state": "open",
    "type": "Issue",
    "owner": "demo_user",
    "repository": "demo_repo",
    "assignees": ["demo_user"],
    "labels": ["bug"],
    "comments": [{"author": "demo_user", "body": "demo comments"}],
}
MOCK_RESPONSE_PULL = {
    "_id": "1313560788",
    "_timestamp": "2023-04-24T08:44:00Z",
    "number": 2,
    "html_url": "https://github.com/demo_user/demo_repo/pull/2",
    "created_at": "2023-04-24T08:44:00Z",
    "closed_at": "2023-04-25T08:44:00Z",
    "title": "update hello world",
    "state": "open",
    "merged_at": "2023-04-25T08:44:00Z",
    "merge_commit_sha": "2123bad7d8sfsa4fds54",
    "body": "Update hello world",
    "type": "Pull request",
    "owner": "demo_user",
    "repository": "demo_repo",
    "head_label": "branch1",
    "base_label": "main",
    "assignees": ["demo_user"],
    "requested_reviewers": ["test_user"],
    "requested_teams": ["team1"],
    "labels": ["V8.8"],
    "review_comments": [{"author": "demo_user", "body": "review comments"}],
    "comments": [{"author": "demo_user", "body": "issue comments"}],
    "reviews": [{"author": "demo_user", "body": "demo comments", "state": "Commented"}],
}
MOCK_RESPONSE_ATTACHMENTS = (
    {
        "_id": "demo_repo/source/source.txt",
        "_timestamp": "2023-04-17T12:55:01Z",
        "name": "source.txt",
        "size": 19,
        "type": "blob",
    },
    {
        "name": "source.txt",
        "path": "source/source.txt",
        "sha": "c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
        "size": 19,
        "url": "https://api.github.com/repos/demo_user/demo_repo/contents/source/source.txt?ref=main",
        "html_url": "https://github.com/demo_user/demo_repo/blob/main/source/source.txt",
        "git_url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/c36b795f98fc9c188fc6gd5a4795vc6j6e0y69a37",
        "download_url": "https://raw.githubusercontent.com/demo_user/demo_repo/main/source/source.txt",
        "type": "file",
        "content": "VGVzdCBGaWxlICEhISDwn5iCCg==\n",
        "encoding": "base64",
        "_timestamp": "2023-04-17T12:55:01Z",
    },
)
MOCK_ATTACHMENT = {
    "path": "source/source.txt",
    "mode": "100644",
    "type": "blob",
    "sha": "36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
    "size": 30,
    "url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
    "_timestamp": "2023-04-17T12:55:01Z",
    "repo_name": "demo_repo",
    "name": "source.txt",
    "extension": ".txt",
}
MOCK_TREE = {
    "sha": "88e3af5daf88e64b273648h37gdf6a561d7092be",
    "url": "https://api.github.com/repos/demo_user/demo_repo/git/trees/88e3af5daf88e64b273648h37gdf6a561d7092be",
    "tree": [
        {
            "path": "source/source.txt",
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


def test_get_default_configuration():
    config = DataSourceConfiguration(GitHubDataSource.get_default_configuration())

    assert config["repositories"] == ["*"]
    assert config["github_token"] == "changeme"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["repositories", "github_token"])
async def test_validate_config_missing_fields_then_raise(field):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.configuration.set_field(name=field, value="")

        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client._get_client.getitem = mock.AsyncMock(
            return_value={"user": "username"}
        )
        await source.ping()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_ping_with_unsuccessful_connection(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        with mock.patch.object(
            source.github_client,
            "ping",
            side_effect=Exception("Something went wrong"),
        ):
            with pytest.raises(Exception):
                await source.ping()


@pytest.mark.asyncio
async def test_close_without_session():
    source = create_source(GitHubDataSource)
    await source.close()


@pytest.mark.asyncio
async def test_close_with_session():
    source = create_source(GitHubDataSource)
    source.github_client._get_client  # noqa
    await source.close()
    assert hasattr(source, "_get_client") is False


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_validate_config_with_invalid_token_then_raise(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client._get_client._request = mock.AsyncMock(
            return_value=(200, {"X-OAuth-Scopes": ""}, {"user": "username"})
        )
        with pytest.raises(
            ConfigurableFieldValueError,
            match="Configured token does not have required rights to fetch the content",
        ):
            await source.validate_config()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_validate_config_with_unauthorized_user(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client.get_response_headers = mock.AsyncMock(
            return_value={"X-OAuth-Scopes": ""}
        )
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_validate_config_with_inaccessible_repositories_then_raise(
    mock_apply_retry_strategy,
):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client.repos = ["repo1", "owner1/repo1", "repo2", "owner2/repo2"]
        source.github_client.get_response_headers = mock.AsyncMock(
            return_value={"X-OAuth-Scopes": "repo"}
        )
        source.get_invalid_repos = mock.AsyncMock(return_value=["repo2"])
        with pytest.raises(ConfigurableFieldValueError):
            await source.validate_config()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_invalid_repos_with_max_retries(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        with pytest.raises(Exception):
            source.github_client.get_paginated_response = Exception()
            await source.get_invalid_repos()


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_get_response_headers_with_rate_limit_exceeded(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        with mock.patch.object(
            source.github_client._get_client,
            "_request",
            side_effect=[
                (
                    403,
                    {"X-OAuth-Scopes": ""},
                    b'{"message": "API rate limit is exceeded"}',
                ),
                (200, {"X-OAuth-Scopes": ""}, {"user": "username"}),
            ],
        ):
            source.github_client._get_retry_after = mock.AsyncMock(return_value=0)
            await source.github_client.get_response_headers(
                method="GET", url=github.ENDPOINTS["USER"]
            )


@pytest.mark.asyncio
async def test_get_retry_after():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client._get_client.getitem = mock.AsyncMock(
            {"resource": {"code": {"reset": 1686563525}}}
        )
        await source.github_client._get_retry_after()


@pytest.mark.asyncio
async def test_get_paginated_response():
    expected_response = {"name": "repo1"}
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        with mock.patch.object(
            source.github_client, "get_response", side_effect=[[{"name": "repo1"}], []]
        ):
            async for data in source.github_client.get_paginated_response(
                github.ENDPOINTS["ALL_REPOS"]
            ):
                assert expected_response == data


@pytest.mark.asyncio
async def test_get_invalid_repos():
    expected_response = ["owner1/repo2", "owner2/repo2"]
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.repos = ["repo1", "owner1/repo1", "repo2", "owner2/repo2"]
        source.github_client.get_user_repos = mock.Mock(
            return_value=AsyncIterator([{"full_name": "owner1/repo1"}])
        )
        with mock.patch.object(
            source.github_client,
            "get_response",
            side_effect=[
                {"login": "owner1"},
                {"name": "repo1"},
                BadRequest(status_code=HTTPStatus.NOT_FOUND),
            ],
        ):
            invalid_repos = await source.get_invalid_repos()
            assert expected_response == invalid_repos


@pytest.mark.asyncio
async def test_get_content_with_text_file():
    expected_response = {
        "_id": "demo_repo/source.txt",
        "_timestamp": "2023-04-17T12:55:01Z",
        "_attachment": "VGVzdCBGaWxlICEhISDwn5iCCg==",
    }
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        with mock.patch.object(
            source.github_client,
            "get_response",
            side_effect=[MOCK_RESPONSE_ATTACHMENTS[1]],
        ):
            actual_response = await source.get_content(
                attachment=MOCK_ATTACHMENT, doit=True
            )
            assert actual_response == expected_response


@pytest.mark.asyncio
async def test_get_content_with_file_size_zero():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        attachment_with_size_zero = MOCK_ATTACHMENT.copy()
        attachment_with_size_zero["size"] = 0
        response = await source.get_content(
            attachment=attachment_with_size_zero, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_with_file_without_extension():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        attachment_without_extension = MOCK_ATTACHMENT.copy()
        attachment_without_extension["name"] = "demo"
        attachment_without_extension["extension"] = ""
        response = await source.get_content(
            attachment=attachment_without_extension, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_with_large_file_size():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        attachment_with_large_size = MOCK_ATTACHMENT.copy()
        attachment_with_large_size["size"] = 23000000
        response = await source.get_content(
            attachment=attachment_with_large_size, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_get_content_with_unsupported_tika_file_type_then_skip():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        attachment_with_unsupported_tika_extension = MOCK_ATTACHMENT.copy()
        attachment_with_unsupported_tika_extension["name"] = "screenshot.png"
        attachment_with_unsupported_tika_extension["extension"] = ".png"
        response = await source.get_content(
            attachment=attachment_with_unsupported_tika_extension, doit=True
        )
        assert response is None


@pytest.mark.asyncio
async def test_fetch_repos():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_repo = MOCK_RESPONSE_REPO.copy()
        mock_repo["id"] = mock_repo.pop("_id")
        mock_repo["updated_at"] = mock_repo.pop("_timestamp")
        mock_repo["owner"] = {"login": "demo_user"}
        mock_repo["default_branch"] = "main"
        source.github_client.get_paginated_response = mock.Mock(
            return_value=AsyncIterator([mock_repo])
        )
        async for repo in source.fetch_repos():
            assert repo == MOCK_RESPONSE_REPO


@pytest.mark.asyncio
async def test_fetch_repos_when_user_repos_is_available():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_repo = MOCK_RESPONSE_REPO.copy()
        mock_repo["id"] = mock_repo.pop("_id")
        mock_repo["updated_at"] = mock_repo.pop("_timestamp")
        mock_repo["owner"] = {"login": "demo_user"}
        mock_repo["default_branch"] = "main"
        source.user_repos = {"demo_user/mock_repo": mock_repo}
        async for repo in source.fetch_repos():
            assert repo == MOCK_RESPONSE_REPO


@pytest.mark.asyncio
async def test_fetch_repos_with_unauthorized_exception():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.get_paginated_response = mock.Mock(
            side_effect=UnauthorizedException()
        )
        with pytest.raises(UnauthorizedException):
            async for _ in source.fetch_repos():
                pass


@pytest.mark.asyncio
async def test_fetch_repos_with_configured_repos():
    user = {"login": "demo_user"}
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.repos = ["demo_repo"]
        source.user = None
        mock_repo = MOCK_RESPONSE_REPO.copy()
        mock_repo["id"] = mock_repo.pop("_id")
        mock_repo["updated_at"] = mock_repo.pop("_timestamp")
        mock_repo["owner"] = user
        with mock.patch.object(
            source.github_client,
            "get_response",
            side_effect=[user, mock_repo],
        ):
            async for repo in source.fetch_repos():
                assert repo == MOCK_RESPONSE_REPO


@pytest.mark.asyncio
async def test_fetch_repos_when_foreign_repo_is_available():
    user = {"login": "demo_user"}
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.repos = ["demo_user/demo_repo"]
        source.user = "demo_user"
        mock_repo = MOCK_RESPONSE_REPO.copy()
        mock_repo["id"] = mock_repo.pop("_id")
        mock_repo["updated_at"] = mock_repo.pop("_timestamp")
        mock_repo["owner"] = user
        source.foreign_repos = {"demo_user/demo_repo": mock_repo}
        async for repo in source.fetch_repos():
            assert repo == MOCK_RESPONSE_REPO


@pytest.mark.asyncio
async def test_fetch_issues():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_issues = MOCK_RESPONSE_ISSUE.copy()
        mock_issues.update(
            {
                "id": mock_issues.pop("_id"),
                "updated_at": mock_issues.pop("_timestamp"),
                "user": {"login": "demo_user"},
                "assignees": [{"login": "demo_user"}],
                "labels": [{"name": "bug"}],
            }
        )
        with mock.patch.object(
            source.github_client,
            "get_paginated_response",
            side_effect=[
                AsyncIterator([mock_issues]),
                AsyncIterator(
                    [{"user": {"login": "demo_user"}, "body": "demo comments"}]
                ),
            ],
        ):
            async for issue in source.fetch_issues("demo_repo"):
                assert issue == MOCK_RESPONSE_ISSUE


@pytest.mark.asyncio
async def test_fetch_issues_with_unauthorized_exception():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.get_paginated_response = mock.Mock(
            side_effect=UnauthorizedException()
        )
        with pytest.raises(UnauthorizedException):
            async for _ in source.fetch_issues("demo_repo"):
                pass


@pytest.mark.asyncio
async def test_fetch_pull_requests():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_pull_request = MOCK_RESPONSE_PULL.copy()
        mock_pull_request["id"] = mock_pull_request.pop("_id")
        mock_pull_request.update(
            {
                "updated_at": mock_pull_request.pop("_timestamp"),
                "user": {"login": "demo_user"},
                "head": {"label": "branch1"},
                "base": {"label": "main"},
                "assignees": [{"login": "demo_user"}],
                "requested_reviewers": [{"login": "test_user"}],
                "requested_teams": [{"name": "team1"}],
                "labels": [{"name": "V8.8"}],
            }
        )
        with mock.patch.object(
            source.github_client,
            "get_paginated_response",
            side_effect=[
                AsyncIterator([mock_pull_request]),
                AsyncIterator(
                    [{"user": {"login": "demo_user"}, "body": "review comments"}]
                ),
                AsyncIterator(
                    [{"user": {"login": "demo_user"}, "body": "issue comments"}]
                ),
                AsyncIterator(
                    [
                        {
                            "user": {"login": "demo_user"},
                            "body": "demo comments",
                            "state": "Commented",
                        }
                    ]
                ),
            ],
        ):
            async for pull in source.fetch_pull_requests("demo_repo"):
                assert pull == MOCK_RESPONSE_PULL


@pytest.mark.asyncio
async def test_fetch_pull_requests_with_unauthorized_exception():
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.github_client.get_paginated_response = mock.Mock(
            side_effect=UnauthorizedException()
        )
        with pytest.raises(UnauthorizedException):
            async for _ in source.fetch_pull_requests("demo_repo"):
                pass


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_fetch_repos_with_max_retries(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client._get_client.getitem = mock.Mock(
            side_effect=BadRequest(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        )
        async for _ in source.fetch_repos():
            pass
        assert mock_apply_retry_strategy.call_count == 2


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_fetch_pull_requests_with_max_retries(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client._get_client.getitem = mock.Mock(
            side_effect=BadRequest(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        )
        async for _ in source.fetch_pull_requests("demo_repo"):
            pass
        assert mock_apply_retry_strategy.call_count == 2


@pytest.mark.asyncio
@mock.patch("connectors.utils.apply_retry_strategy")
async def test_fetch_issues_with_max_retries(mock_apply_retry_strategy):
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        mock_apply_retry_strategy.return_value = mock.Mock()
        source.github_client._get_client.getitem = mock.Mock(
            side_effect=BadRequest(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
        )
        async for _ in source.fetch_issues("demo_repo"):
            pass
        assert mock_apply_retry_strategy.call_count == 2


@pytest.mark.asyncio
async def test_fetch_files():
    expected_response = (
        {
            "name": "source.txt",
            "size": 30,
            "type": "blob",
            "path": "source/source.txt",
            "mode": "100644",
            "extension": ".txt",
            "_timestamp": "2023-04-17T12:55:01Z",
            "_id": "demo_repo/source/source.txt",
        },
        {
            "path": "source/source.txt",
            "mode": "100644",
            "type": "blob",
            "sha": "36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
            "size": 30,
            "url": "https://api.github.com/repos/demo_user/demo_repo/git/blobs/36888b54c2a2f75tfbf2b7e7e95f68d0g8911ccb",
            "_timestamp": "2023-04-17T12:55:01Z",
            "repo_name": "demo_repo",
            "name": "source.txt",
            "extension": ".txt",
        },
    )
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        with mock.patch.object(
            source.github_client,
            "get_response",
            side_effect=[MOCK_TREE, MOCK_COMMITS],
        ):
            async for document in source.fetch_files("demo_repo", "main"):
                assert expected_response == document


@pytest.mark.asyncio
async def test_get_docs():
    expected_response = [
        MOCK_RESPONSE_REPO,
        MOCK_RESPONSE_PULL,
        MOCK_RESPONSE_ISSUE,
        MOCK_RESPONSE_ATTACHMENTS[0],
    ]
    actual_response = []
    async with AsyncSourceContextManager(GitHubDataSource) as source:
        source.fetch_repos = mock.Mock(return_value=AsyncIterator([MOCK_RESPONSE_REPO]))
        source.fetch_issues = mock.Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_ISSUE])
        )
        source.fetch_pull_requests = mock.Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_PULL])
        )
        source.fetch_files = mock.Mock(
            return_value=AsyncIterator([MOCK_RESPONSE_ATTACHMENTS])
        )
        async for document, _ in source.get_docs():
            actual_response.append(document)
        assert expected_response == actual_response
