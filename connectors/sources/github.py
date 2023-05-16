#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitHub source module responsible to fetch documents from GitHub Cloud and Server."""
import json
from functools import cached_property

import aiohttp
from gidgethub import BadRequest
from gidgethub.aiohttp import GitHubAPI

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import CancellableSleeps, RetryStrategy, iso_utc, retryable

RETRIES = 3
RETRY_INTERVAL = 2

USER_API = "https://api.github.com/user"
ENDPOINTS = {
    "ALL_REPOS": "/user/repos?type=all",
    "REPO": "repos/{repo_name}",
    "USER": "/user",
}


class GitHubClient:
    """GitHub Client"""

    def __init__(self, configuration):
        """Setup the GitHub client.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        self.session = None
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.repos = self.configuration["repositories"]
        self.github_token = self.configuration["github_token"]
        self.headers = {"Authorization": f"Bearer {self.github_token}"}

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def execute_getitem(self, request):
        """Execute request using getitem method of GitHubAPI.

        Args:
            request (str): API for the request

        Returns:
            list: Response of the request
        """
        return await self._get_session.getitem(url=request)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def execute_request(self, method, url):
        """Execute request using _request method of GitHubAPI.

        Args:
            request (str): API for the request

        Returns:
            list: Response of the request
        """
        status_code, response_headers, data = await self._get_session._request(
            method="get", url=USER_API, headers=self.headers
        )
        if status_code == 200:
            return response_headers
        data = data.decode("utf-8")
        message = json.loads(data)
        raise Exception(
            f"Something went wrong while executing request Exception: {message['message']}"
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def execute_getiter(self, request):
        """Execute request using getiter method of GitHubAPI.

        Args:
            request (str): API for the request

        Yields:
            dict: Response of the request
        """
        async for response in self._get_session.getiter(url=request):
            yield response

    async def filter_repos(self):
        try:
            logger.debug("Filtering repositories")
            public_repos, configured_repos, users_repos = [], [], []
            for repo in self.repos:
                if "/" in repo:
                    public_repos.append(repo)
                else:
                    configured_repos.append(repo)

            async for response in self.execute_getiter(request=ENDPOINTS["ALL_REPOS"]):
                users_repos.append(response["name"])
            invalid_repo = list(set(configured_repos) - set(users_repos))

            for repo in public_repos:
                try:
                    await self.execute_getitem(
                        request=ENDPOINTS["REPO"].format(repo_name=repo)
                    )
                except BadRequest:
                    invalid_repo.append(repo)
            return invalid_repo
        except Exception as exception:
            logger.exception(
                f"Error while filtering repositories. Exception: {exception}."
            )
            raise

    @cached_property
    def _get_session(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return GitHubAPI(
            self.session,
            requester="",
            oauth_token=self.github_token,
        )

    async def ping(self):
        await self.execute_getitem(request=ENDPOINTS["USER"])

    async def close_session(self):
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        del self._get_session


class GitHubDataSource(BaseDataSource):
    """GitHub"""

    name = "GitHub"
    service_type = "github"

    def __init__(self, configuration):
        """Setup the connection to the GitHub instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self._sleeps = CancellableSleeps()
        self.github_client = GitHubClient(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GitHub.

        Returns:
            dict: Default configuration.
        """
        return {
            "github_token": {
                "label": "GitHub Token",
                "sensitive": True,
                "order": 1,
                "type": "str",
                "value": "changeme",
            },
            "repositories": {
                "display": "textarea",
                "label": "List of repositories",
                "order": 2,
                "type": "list",
                "value": "*",
            },
            "retry_count": {
                "display_value": RETRIES,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 3,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
                "validations": [{"type": "less_than", "constraint": 10}],
            },
        }

    async def _remote_validation(self):
        """Validate scope of the configured Token and accessibility of repositories

        Raises:
            ConfigurableFieldValueError: Insufficient privileges error.
        """
        response_headers = await self.github_client.execute_request(
            method="get", url=USER_API
        )
        if "repo" not in response_headers.get("X-OAuth-Scopes"):  # pyright: ignore
            raise ConfigurableFieldValueError(
                "Configured token does not have required rights to fetch the content"
            )
        if self.github_client.repos != ["*"]:
            invalid_repos = await self.github_client.filter_repos()
            if invalid_repos:
                raise ConfigurableFieldValueError(
                    f"Inaccessible repositories '{', '.join(invalid_repos)}'."
                )

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured repositories are accessible or not and scope of the token
        """
        self.configuration.check_valid()
        await self._remote_validation()

    async def close(self):
        await self.github_client.close_session()

    async def ping(self):
        try:
            await self.github_client.ping()
            logger.debug("Successfully connected to the GitHub.")

        except Exception:
            logger.exception("Error while connecting to the GitHub.")
            raise

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GitHub objects in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GitHub.
        """
        # yield dummy document to make the chunked PR work, subsequent PR will replace it with actual implementation
        yield {"_id": "123", "timestamp": iso_utc()}, None
