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
from connectors.sources.github import GitHubDataSource
from connectors.sources.tests.support import create_source
from connectors.tests.commons import AsyncIterator


def test_get_configuration():
    config = DataSourceConfiguration(GitHubDataSource.get_default_configuration())

    assert config["repositories"] == ["*"]
    assert config["github_token"] == "changeme"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["repositories", "github_token"])
async def test_validate_config_missing_fields_then_raise(field):
    source = create_source(GitHubDataSource)
    source.configuration.set_field(name=field, value="")

    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_ping_with_successful_connection():
    source = create_source(GitHubDataSource)
    source.github_client._get_session.getitem = mock.AsyncMock(
        return_value={"user": "username"}
    )
    await source.ping()


@pytest.mark.asyncio
async def test_ping_with_unsuccessful_connection():
    source = create_source(GitHubDataSource)

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
    source.github_client._get_session
    await source.close()


@pytest.mark.asyncio
async def test_close_with_session():
    source = create_source(GitHubDataSource)
    source.github_client._get_session
    await source.close()


@pytest.mark.asyncio
async def test_validate_config_with_invalid_token_then_raise():
    source = create_source(GitHubDataSource)
    source.github_client._get_session._request = mock.AsyncMock(
        return_value=(200, {"X-OAuth-Scopes": ""}, {"user": "username"})
    )
    with pytest.raises(
        ConfigurableFieldValueError,
        match="Configured token does not have required rights to fetch the content",
    ):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_unauthorized_user():
    source = create_source(GitHubDataSource)
    source.github_client.execute_request = mock.AsyncMock(
        return_value={"X-OAuth-Scopes": ""}
    )
    with pytest.raises(Exception):
        await source.validate_config()


@pytest.mark.asyncio
async def test_validate_config_with_inaccessible_repositories_then_raise():
    source = create_source(GitHubDataSource)
    source.github_client.repos = ["repo1", "owner1/repo1", "repo2", "owner2/repo2"]
    source.github_client.execute_request = mock.AsyncMock(
        return_value={"X-OAuth-Scopes": "repo"}
    )
    source.github_client.filter_repos = mock.AsyncMock(return_value=["repo2"])
    with pytest.raises(ConfigurableFieldValueError):
        await source.validate_config()


@pytest.mark.asyncio
async def test_filter_repos_with_max_retries():
    source = create_source(GitHubDataSource)
    with pytest.raises(Exception):
        with mock.patch.object(
            source.github_client,
            "execute_getiter",
            side_effect=Exception("Something went wrong"),
        ):
            await source.github_client.filter_repos()


@pytest.mark.asyncio
async def test_execute_getiter():
    expected_response = {"name": "repo1"}
    source = create_source(GitHubDataSource)
    source.github_client._get_session.getiter = mock.Mock(
        return_value=AsyncIterator([{"name": "repo1"}])
    )
    async for data in source.github_client.execute_getiter(
        github.ENDPOINTS["ALL_REPOS"]
    ):
        assert expected_response == data


@pytest.mark.asyncio
async def test_filter_repos():
    expected_response = ["repo2", "owner2/repo2"]
    source = create_source(GitHubDataSource)
    source.github_client.repos = ["repo1", "owner1/repo1", "repo2", "owner2/repo2"]
    source.github_client.execute_getiter = mock.Mock(
        return_value=AsyncIterator([{"name": "repo1"}])
    )
    with mock.patch.object(
        source.github_client,
        "execute_getitem",
        side_effect=[{"name": "repo1"}, BadRequest(status_code=HTTPStatus.NOT_FOUND)],
    ):
        invalid_repos = await source.github_client.filter_repos()
        assert expected_response == invalid_repos
