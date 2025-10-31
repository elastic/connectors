#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import json
import time
from functools import cached_property

import aiohttp
import gidgethub
from connectors_sdk.logger import logger
from connectors_sdk.utils import nested_get_from_dict
from gidgethub import QueryError, sansio
from gidgethub.abc import (
    BadGraphQLRequest,
    GraphQLAuthorizationFailure,
)
from gidgethub.aiohttp import GitHubAPI
from gidgethub.apps import get_installation_access_token, get_jwt

from connectors.sources.github.query import GithubQuery
from connectors.sources.github.utils import (
    FORBIDDEN,
    GITHUB_APP,
    PERSONAL_ACCESS_TOKEN,
    REPOSITORY_OBJECT,
    RETRIES,
    RETRY_INTERVAL,
    UNAUTHORIZED,
    ForbiddenException,
    NoInstallationAccessTokenException,
    UnauthorizedException,
)
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    retryable,
    ssl_context,
)


class GitHubClient:
    def __init__(
        self, auth_method, base_url, app_id, private_key, token, ssl_enabled, ssl_ca
    ):
        self._sleeps = CancellableSleeps()
        self._logger = logger
        self.auth_method = auth_method
        self.base_url = base_url
        self.app_id = app_id if self.auth_method == GITHUB_APP else None
        self.private_key = private_key if self.auth_method == GITHUB_APP else None
        self._personal_access_token = (
            token if self.auth_method == PERSONAL_ACCESS_TOKEN else None
        )
        self._installation_access_token = None

        if self.base_url == "https://api.github.com":
            self.endpoints = {
                "TREE": "/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
                "COMMITS": "/repos/{repo_name}/commits?path={path}",
                "PATH": "/repos/{repo_name}/contents/{path}",
            }
        else:
            self.endpoints = {
                "TREE": "/api/v3/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
                "COMMITS": "api/v3/repos/{repo_name}/commits?path={path}",
                "PATH": "api/v3/repos/{repo_name}/contents/{path}",
            }
        if ssl_enabled and ssl_ca:
            self.ssl_ctx = ssl_context(certificate=ssl_ca)
        else:
            self.ssl_ctx = False

        # a variable to hold the current installation id, used to refresh the access token
        self._installation_id = None

    def set_logger(self, logger_):
        self._logger = logger_

    async def _get_retry_after(self, resource_type):
        current_time = time.time()
        response = await self.get_github_item("/rate_limit")
        reset = nested_get_from_dict(
            response, ["resources", resource_type, "reset"], default=current_time
        )
        # Adding a 5 second delay to account for server delays
        return (reset - current_time) + 5  # pyright: ignore

    async def _put_to_sleep(self, resource_type):
        retry_after = await self._get_retry_after(resource_type=resource_type)
        self._logger.debug(
            f"Rate limit exceeded. Retry after {retry_after} seconds. Resource type: {resource_type}"
        )
        await self._sleeps.sleep(retry_after)
        msg = "Rate limit exceeded."
        raise Exception(msg)

    def _access_token(self):
        if self.auth_method == PERSONAL_ACCESS_TOKEN:
            return self._personal_access_token
        if not self._installation_access_token:
            raise NoInstallationAccessTokenException
        return self._installation_access_token

    # update the current installation id and re-generate access token
    async def update_installation_id(self, installation_id):
        self._logger.debug(
            f"Updating installation id - new ID: {installation_id}, original ID: {self._installation_id}"
        )
        self._installation_id = installation_id
        await self._update_installation_access_token()

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _update_installation_access_token(self):
        try:
            access_token_response = await get_installation_access_token(
                gh=self._get_client,
                installation_id=self._installation_id,  # type: ignore[arg-type]
                app_id=self.app_id,  # type: ignore[arg-type]
                private_key=self.private_key,  # type: ignore[arg-type]
            )
            self._installation_access_token = access_token_response["token"]
        except gidgethub.RateLimitExceeded:
            await self._put_to_sleep("core")
        except Exception:
            self._logger.exception(
                f"Failed to get access token for installation {self._installation_id}.",
                exc_info=True,
            )
            raise

    @cached_property
    def _get_session(self):
        connector = aiohttp.TCPConnector(ssl=self.ssl_ctx)
        timeout = aiohttp.ClientTimeout(total=None)
        return aiohttp.ClientSession(
            timeout=timeout,
            raise_for_status=False,
            connector=connector,
        )

    @cached_property
    def _get_client(self):
        return GitHubAPI(
            session=self._get_session,
            requester="",
            base_url=self.base_url,
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def graphql(
        self,
        query: str,
        variables: dict | None = None,
        ignore_errors: list[str] | None = None,
    ) -> dict:
        """Invoke GraphQL request to fetch repositories, pull requests, and issues.

        Args:
            query: Dictionary comprising of query for the GraphQL request.
            variables: Dictionary comprising of query for the GraphQL request.
            ignore_errors: List of error types to ignore and return null for instead of raising exceptions.

        Raises:
            UnauthorizedException: Unauthorized exception
            exception: An instance of an exception class.

        Yields:
            dictionary: Client response
        """

        ignore_errors = ignore_errors or []

        url = f"{self.base_url}/graphql"
        self._logger.debug(
            f"Sending POST to {url} with query: '{json.dumps(query)}' and variables: '{json.dumps(variables)}'"
        )
        try:
            self._get_client.oauth_token = self._access_token()
            return await self._get_client.graphql(
                query=query, endpoint=url, **variables or {}
            )
        except GraphQLAuthorizationFailure as exception:
            if self.auth_method == GITHUB_APP:
                self._logger.debug(
                    f"The access token for installation #{self._installation_id} expired, Regenerating a new token."
                )
                await self._update_installation_access_token()
                raise
            msg = "Your Github token is either expired or revoked. Please check again."
            raise UnauthorizedException(msg) from exception
        except BadGraphQLRequest as exception:
            if exception.status_code == FORBIDDEN:
                msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {url} and query: {query}."
                raise ForbiddenException(msg) from exception
            else:
                raise
        except QueryError as exception:
            errors = exception.response.get("errors", [])
            for error in errors:
                if (
                    error.get("type", "").lower() == "rate_limited"
                    and "api rate limit exceeded" in error.get("message").lower()
                ):
                    await self._put_to_sleep(resource_type="graphql")

            if all(error.get("type") in ignore_errors for error in errors):
                # All errors are ignored, return just the data part without errors
                return exception.response.get("data", {})

            # report only non-ignored errors
            non_ignored_errors = [
                error for error in errors if error.get("type") not in ignore_errors
            ]

            msg = f"Error while executing query. Exception: {non_ignored_errors}"
            raise Exception(msg) from exception
        except Exception as e:
            self._logger.debug(
                f"An unexpected error occurred while executing GraphQL query: {query}. Error: {e}"
            )
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def get_github_item(self, resource):
        """Execute request using getitem method of GitHubAPI which is using REST API.
        Using Rest API for fetching files and folder along with content.

        Args:
            resource (str): API to get the response

        Returns:
            dict/list: Response of the request
        """
        self._logger.debug(f"Getting github item: {resource}")
        try:
            return await self._get_client.getitem(
                url=resource, oauth_token=self._access_token()
            )
        except gidgethub.RateLimitExceeded:
            await self._put_to_sleep("core")
        except gidgethub.HTTPException as exception:
            if exception.status_code == UNAUTHORIZED:
                if self.auth_method == GITHUB_APP:
                    self._logger.debug(
                        f"The access token for installation #{self._installation_id} expired, Regenerating a new token."
                    )
                    await self._update_installation_access_token()
                    raise
                msg = "Your Github token is either expired or revoked. Please check again."
                raise UnauthorizedException(msg) from exception
            elif exception.status_code == FORBIDDEN:
                msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {resource}."
                raise ForbiddenException(msg) from exception
            else:
                raise
        except Exception as e:
            self._logger.debug(
                f"An unexpected error occurred while getting GitHub item: {resource}. Error: {e}"
            )
            raise

    async def paginated_api_call(self, query, variables, keys):
        """Make a paginated API call for fetching GitHub objects.

        Args:
            query (string): Graphql Query
            variables (dict): Variables for Graphql API
            keys (list): List of fields to get pageInfo

        Yields:
            dict: dictionary containing response of GitHub.
        """
        while True:
            response = await self.graphql(query=query, variables=variables)
            yield response

            page_info = nested_get_from_dict(response, keys + ["pageInfo"], default={})
            if not page_info.get("hasNextPage"):
                break
            variables["cursor"] = page_info["endCursor"]  # pyright: ignore

    def get_repo_details(self, repo_name):
        return repo_name.split("/")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=UnauthorizedException,
    )
    async def get_personal_access_token_scopes(self):
        try:
            request_headers = sansio.create_headers(
                self._get_client.requester,
                accept=sansio.accept_format(),
                oauth_token=self._access_token(),
            )
            url = f"{self.base_url}/graphql"
            _, headers, _ = await self._get_client._request(
                "HEAD", url, request_headers
            )
            scopes = headers.get("X-OAuth-Scopes")
            if not scopes or not scopes.strip():
                self._logger.warning(
                    f"Couldn't find 'X-OAuth-Scopes' in headers {headers}"
                )
                return set()
            return {scope.strip() for scope in scopes.split(",")}
        except gidgethub.HTTPException as exception:
            if exception.status_code == FORBIDDEN:
                msg = f"Provided GitHub token does not have the necessary permissions to perform the request for the URL: {self.base_url}."
                raise ForbiddenException(msg) from exception
            elif exception.status_code == UNAUTHORIZED:
                msg = "Your Github token is either expired or revoked. Please check again."
                raise UnauthorizedException(msg) from exception
            else:
                raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _github_app_get(self, url):
        self._logger.debug(f"Making a get request to GitHub: {url}")
        try:
            return await self._get_client._make_request(
                "GET",
                url,
                {},
                b"",
                sansio.accept_format(),
                get_jwt(app_id=self.app_id, private_key=self.private_key),  # type: ignore[arg-type]
            )
        # we don't expect any 401 error as the jwt is freshly generated
        except gidgethub.RateLimitExceeded:
            await self._put_to_sleep("core")
        except Exception:
            raise

    async def _github_app_paginated_get(self, url):
        data, more = await self._github_app_get(url)  # pyright: ignore
        if data:
            for item in data:
                yield item
        if more:
            async for item in self._github_app_paginated_get(more):  # pyright: ignore
                yield item

    async def get_installations(self):
        async for installation in self._github_app_paginated_get(
            url="/app/installations"
        ):
            if installation["suspended_at"]:  # type: ignore[index]
                self._logger.debug(
                    f"Skip installation '{installation['id']}' because it's suspended."  # type: ignore[index]
                )
                continue
            yield installation

    async def get_org_repos(self, org_name):
        repo_variables = {
            "orgName": org_name,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.ORG_REPOS_QUERY.value,
            variables=repo_variables,
            keys=["organization", "repositories"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response, ["organization", "repositories", "nodes"], default=[]
            ):
                yield repo

    async def get_user_repos(self, user):
        repo_variables = {
            "login": user,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.REPOS_QUERY.value,
            variables=repo_variables,
            keys=["user", "repositories"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response, ["user", "repositories", "nodes"], default=[]
            ):
                yield repo

    async def get_foreign_repo(self, repo_name):
        owner, repo = self.get_repo_details(repo_name=repo_name)
        repo_variables = {"owner": owner, "repositoryName": repo}
        data = await self.graphql(
            query=GithubQuery.REPO_QUERY.value, variables=repo_variables
        )
        return data.get(REPOSITORY_OBJECT)

    async def get_repos_by_fully_qualified_name_batch(
        self, repo_names: list[str], batch_size: int = 15
    ) -> dict[str, dict | None]:
        """Batch validate multiple repositories by fully qualified name in fewer GraphQL requests.

        Args:
            repo_names (list): List of fully qualified repository names (owner/repo format) to validate
            batch_size (int): Number of repos to validate per request (default 15)

        Returns:
            dict: Dictionary mapping repo_name to repository object (or None if invalid)
        """
        results = {}

        # Process repositories in batches to avoid hitting GraphQL complexity limits
        for i in range(0, len(repo_names), batch_size):
            batch = repo_names[i : i + batch_size]
            batch_results = await self._fetch_repos_batch(batch)
            results.update(batch_results)

        return results

    async def _fetch_repos_batch(self, repo_names: list[str]) -> dict[str, dict | None]:
        """Fetch a batch of repositories in a single GraphQL query using aliases.

        Note: This method will NOT raise exceptions for non-existent repositories.
        Invalid/non-existent repositories will be returned as None values in the result.
        """
        if not repo_names:
            return {}

        query_parts = []
        variables = {}

        for i, repo_name in enumerate(repo_names):
            owner, repo = self.get_repo_details(repo_name=repo_name)
            alias = f"repo{i}"

            variables[f"{alias}_owner"] = owner
            variables[f"{alias}_name"] = repo

            query_part = f"""
            {alias}: repository(owner: ${alias}_owner, name: ${alias}_name) {{
                id
                updatedAt
                name
                nameWithOwner
                url
                description
                visibility
                primaryLanguage {{
                    name
                }}
                defaultBranchRef {{
                    name
                }}
                isFork
                stargazerCount
                watchers {{
                    totalCount
                }}
                forkCount
                createdAt
                isArchived
            }}"""
            query_parts.append(query_part)

        variable_declarations = [
            f"${var_name}: String!" for var_name in variables.keys()
        ]

        query = GithubQuery.BATCH_REPO_QUERY_TEMPLATE.value.format(
            batch_queries=", ".join(variable_declarations),
            query_body=" ".join(query_parts),
        )

        data = await self.graphql(
            query=query, variables=variables, ignore_errors=["NOT_FOUND"]
        )

        # Map results back to repo names
        results = {
            repo_name: data.get(f"repo{i}") for i, repo_name in enumerate(repo_names)
        }

        return results

    async def _fetch_all_members(self, org_name):
        org_variables = {
            "orgName": org_name,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            query=GithubQuery.ORG_MEMBERS_QUERY.value,
            variables=org_variables,
            keys=["organization", "membersWithRole"],
        ):
            for repo in nested_get_from_dict(  # pyright: ignore
                response,
                ["organization", "membersWithRole", "edges"],
                default=[],
            ):
                yield repo.get("node")

    async def get_logged_in_user(self):
        data = await self.graphql(query=GithubQuery.USER_QUERY.value)
        return nested_get_from_dict(data, ["viewer", "login"])

    async def ping(self):
        if self.auth_method == GITHUB_APP:
            await self._github_app_get(url="/app")
        else:
            await self.get_logged_in_user()

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def bifurcate_repos(self, repos, owner):
        foreign_repos, configured_repos = [], []
        for repo_name in repos:
            if repo_name not in ["", None]:
                if "/" in repo_name:
                    foreign_repos.append(repo_name)
                else:
                    configured_repos.append(f"{owner}/{repo_name}")
        return foreign_repos, configured_repos
