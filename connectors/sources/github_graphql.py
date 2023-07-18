#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitHub source module responsible to fetch documents from GitHub Cloud and Server."""
import asyncio
import time
from enum import Enum
from functools import cached_property, partial

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError
from gidgethub.aiohttp import GitHubAPI

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.graphql_queries import GithubQuery
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    RetryStrategy,
    convert_to_b64,
    decode_base64_value,
    retryable,
    ssl_context,
)

WILDCARD = "*"
BLOB = "blob"
GITHUB_CLOUD = "github_cloud"
GITHUB_SERVER = "github_server"
PULL_REQUEST_OBJECT = "pullRequest"
REPOSITORY_OBJECT = "repository"

RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760  # ~ 10 Megabytes
PAGE_SIZE = 100
FORBIDDEN = 403

FILE_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "mode": "mode",
    "extension": "extension",
    "_timestamp": "_timestamp",
}


class ObjectType(Enum):
    REPOSITORY = "Repository"
    ISSUE = "Issue"
    PULL_REQUEST = "Pull request"


class UnauthorizedException(Exception):
    pass


class GitHubClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = configuration["data_source"] == GITHUB_CLOUD
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.github_url = f"{configuration['host'].rstrip('/')}/api"
        self.repos = self.configuration["repositories"]
        self.github_token = self.configuration["token"]
        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False
        self.endpoints = {
            "TREE": "/api/v3/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
            "COMMITS": "api/v3/repos/{repo_name}/commits?path={path}",
        }
        if self.is_cloud:
            self.github_url = "https://api.github.com"
            self.endpoints = {
                "TREE": "/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
                "COMMITS": "/repos/{repo_name}/commits?path={path}",
            }
        self.user = None

    def set_logger(self, logger_):
        self._logger = logger_

    def get_rate_limit_encountered(self, status_code, message):
        return status_code == FORBIDDEN and "rate limit" in str(message).lower()

    async def _get_retry_after(self, type):
        current_time = time.time()
        response = await self._get_client.getitem("/rate_limit")
        reset = response.get("resources", {}).get(type, {}).get("reset", current_time)
        # Adding a 5 second delay to account for server delays
        return (reset - current_time) + 5

    async def _put_to_sleep(self, type):
        retry_after = await self._get_retry_after(type=type)
        self._logger.debug(
            f"Connector will attempt to retry after {retry_after} seconds."
        )
        await self._sleeps.sleep(retry_after)
        raise Exception("Rate limit exceeded.")

    @cached_property
    def _get_session(self):
        headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github.raw",
        }
        connector = aiohttp.TCPConnector(ssl=self.ssl_ctx)
        timeout = aiohttp.ClientTimeout(total=None)
        return aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
            raise_for_status=True,
            connector=connector,
        )

    @cached_property
    def _get_client(self):
        return GitHubAPI(
            session=self._get_session,
            requester="",
            oauth_token=self.github_token,
            base_url=self.github_url,
        )

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def api_call(self, query_data, need_headers=False):
        """Invoke GraphQL request to fetch repositories, pull requests, and issues.

        Args:
            query_data: Dictionary comprising of query and variables for the GraphQL request.

        Raises:
            UnauthorizedException: Unauthorized exception
            exception: An instance of an exception class.

        Yields:
            dictionary: Client response
        """
        url = f"{self.github_url}/graphql"
        try:
            async with self._get_session.post(
                url=url, json=query_data, ssl=self.ssl_ctx
            ) as response:
                json_response = await response.json()
                if not json_response.get("errors"):
                    return (
                        (json_response, response.headers)
                        if need_headers
                        else json_response
                    )
                for error in json_response.get("errors"):
                    if (
                        error.get("type") == "RATE_LIMITED"
                        and "api rate limit exceeded" in error.get("message").lower()
                    ):
                        await self._put_to_sleep(type="graphql")
                raise Exception(
                    f"Error while executing query. Exception: {json_response['errors']}"
                )
        except ClientResponseError as exception:
            if exception.status == 401:
                raise UnauthorizedException(
                    "Your Github token is either expired or revoked. Please check again."
                ) from exception
        except Exception:
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_response(self, resource):
        """Execute request using getitem method of GitHubAPI which is using REST API.
        Using Rest API for fetching files and folder along with content.

        Args:
            resource (str): API to get the response

        Returns:
            dict/list: Response of the request
        """
        try:
            return await self._get_client.getitem(url=resource)
        except ClientResponseError as exception:
            if exception.status == 401:
                raise UnauthorizedException(
                    "Your Github token is either expired or revoked. Please check again."
                ) from exception
            elif self.get_rate_limit_encountered(exception.status, exception):
                await self._put_to_sleep("core")
            else:
                raise
        except Exception:
            raise

    def get_page_info(self, response, keys):
        """Get page information from the response.

        Args:
            response (dict): Response of GitHub
            Keys (list): List of fields to get pageInfo

        Return:
            dict: dictionary containing page information.
        """
        current_level = response.get("data")
        for key in keys:
            current_level = current_level.get(key)
            if current_level is None:
                break
        return current_level.get("pageInfo")

    async def paginated_api_call(self, variables, query, keys):
        """Make a paginated API call for fetching GitHub objects.

        Args:
            variables (dict): Variables for Graphql API
            query (string): Graphql Query
            keys (list): List of fields to get pageInfo

        Yields:
            dict: dictionary containing response of GitHub.
        """
        while True:
            query_data = {"query": query, "variables": variables}
            response = await self.api_call(query_data=query_data)
            yield response

            page_info = self.get_page_info(response=response, keys=keys)
            if not page_info.get("hasNextPage"):
                break
            variables["cursor"] = page_info["endCursor"]

    def get_repo_details(self, repo_name):
        return repo_name.split("/")

    async def get_user_repos(self):
        repo_variables = {
            "login": self.user,
            "cursor": None,
        }
        async for response in self.paginated_api_call(
            variables=repo_variables,
            query=GithubQuery.REPOS_QUERY.value,
            keys=["user", "repositories"],
        ):
            for repo in (
                response.get("data", {})  # pyright: ignore
                .get("user", {})
                .get("repositories", {})
                .get("nodes")
            ):
                yield repo

    async def get_foreign_repo(self, repo_name):
        owner, repo = self.get_repo_details(repo_name=repo_name)
        repo_variables = {"owner": owner, "repositoryName": repo}
        query_data = {
            "query": GithubQuery.REPO_QUERY.value,
            "variables": repo_variables,
        }
        repo_response = await self.api_call(query_data=query_data)
        return repo_response.get("data", {}).get(REPOSITORY_OBJECT)  # pyright: ignore

    async def get_logged_in_user(self):
        query_data = {
            "query": GithubQuery.USER_QUERY.value,
            "variables": None,
        }
        response = await self.api_call(query_data=query_data)
        return (
            response.get("data", {}).get("viewer", {}).get("login")  # pyright: ignore
        )

    async def ping(self):
        query_data = {"query": GithubQuery.USER_QUERY.value, "variables": None}
        await self.api_call(query_data=query_data)

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
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
        self.github_client = GitHubClient(configuration=configuration)
        self.user_repos = {}
        self.foreign_repos = {}

    def _set_internal_logger(self):
        self.github_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GitHub.

        Returns:
            dict: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "GitHub data source",
                "options": [
                    {"label": "GitHub Cloud", "value": GITHUB_CLOUD},
                    {"label": "GitHub Server", "value": GITHUB_SERVER},
                ],
                "order": 1,
                "type": "str",
                "value": GITHUB_SERVER,
            },
            "host": {
                "depends_on": [{"field": "data_source", "value": GITHUB_SERVER}],
                "label": "GitHub URL",
                "order": 2,
                "type": "str",
                "value": "http://127.0.0.1:5000",
            },
            "token": {
                "label": "GitHub Token",
                "order": 3,
                "sensitive": True,
                "type": "str",
                "value": "changeme",
            },
            "repositories": {
                "display": "textarea",
                "label": "List of repositories",
                "order": 4,
                "type": "list",
                "value": WILDCARD,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 5,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 6,
                "type": "str",
                "value": "",
            },
            "retry_count": {
                "display_value": RETRIES,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
                "validations": [{"type": "less_than", "constraint": 10}],
            },
        }

    async def get_invalid_repos(self):
        try:
            self._logger.debug(
                "Checking if there are any inaccessible repositories configured"
            )
            foreign_repos, configured_repos = [], []
            self.github_client.user = await self.github_client.get_logged_in_user()

            for repo_name in self.github_client.repos:
                if repo_name not in ["", None]:
                    if "/" in repo_name:
                        foreign_repos.append(repo_name)
                    else:
                        configured_repos.append(
                            f"{self.github_client.user}/{repo_name}"
                        )
            async for repo in self.github_client.get_user_repos():
                self.user_repos[repo["nameWithOwner"]] = repo
            invalid_repos = list(set(configured_repos) - set(self.user_repos.keys()))

            for repo_name in foreign_repos:
                try:
                    self.foreign_repos[
                        repo_name
                    ] = await self.github_client.get_foreign_repo(repo_name=repo_name)
                except Exception:
                    invalid_repos.append(repo_name)
            return invalid_repos
        except Exception as exception:
            self._logger.exception(
                f"Error while checking for inaccessible repositories. Exception: {exception}."
            )
            raise

    async def _remote_validation(self):
        """Validate scope of the configured Token and accessibility of repositories

        Raises:
            ConfigurableFieldValueError: Insufficient privileges error.
        """
        query_data = {"query": GithubQuery.USER_QUERY.value, "variables": None}
        _, headers = await self.github_client.api_call(  # pyright: ignore
            query_data=query_data, need_headers=True
        )
        if "repo" not in headers.get("X-OAuth-Scopes"):  # pyright: ignore
            raise ConfigurableFieldValueError(
                "Configured token does not have required rights to fetch the content"
            )
        if self.github_client.repos != [WILDCARD]:
            invalid_repos = await self.get_invalid_repos()
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
        await self.github_client.close()

    async def ping(self):
        try:
            await self.github_client.ping()
            self._logger.debug("Successfully connected to GitHub.")
        except Exception:
            self._logger.exception("Error while connecting to GitHub.")
            raise

    def adapt_gh_doc_to_es_doc(self, github_document, schema):
        document = {}
        for es_field, github_field in schema.items():
            document[es_field] = github_document[github_field]
        return document

    def _prepare_pull_request_doc(self, pull_request, reviews):
        return {
            "_id": pull_request.pop("id"),
            "_timestamp": pull_request.pop("updatedAt"),
            "type": ObjectType.PULL_REQUEST.value,
            "issue_comments": pull_request.get("comments", {}).get("nodes"),
            "reviews_comments": reviews,
            "labels_field": pull_request.get("labels", {}).get("nodes"),
            "assignees_list": pull_request.get("assignees", {}).get("nodes"),
            "requested_reviewers": pull_request.get("reviewRequests", {}).get("nodes"),
        }

    def _prepare_issue_doc(self, issue):
        return {
            "_id": issue.pop("id"),
            "type": ObjectType.ISSUE.value,
            "_timestamp": issue.pop("updatedAt"),
            "issue_comments": issue.get("comments", {}).get("nodes"),
            "labels_field": issue.get("labels", {}).get("nodes"),
            "assignees_list": issue.get("assignees", {}).get("nodes"),
        }

    def _prepare_review_doc(self, review):
        return {
            "author": review.get("author").get("login"),
            "body": review.get("body"),
            "state": review.get("state"),
            "comments": review.get("comments").get("nodes"),
        }

    async def _get_personal_repos(self):
        if not self.user_repos:
            async for repo_object in self.github_client.get_user_repos():
                self.user_repos[repo_object["nameWithOwner"]] = repo_object
        for repo_object in self.user_repos.values():
            repo_object.update(
                {
                    "_id": repo_object.pop("id"),
                    "_timestamp": repo_object.pop("updatedAt"),
                    "type": ObjectType.REPOSITORY.value,
                }
            )
            yield repo_object

    async def _get_configured_repos(self):
        for repo_name in self.github_client.repos:
            if repo_name in ["", None]:
                continue

            # Converting the local repository names to username/repo_name format.
            if "/" not in repo_name:
                repo_name = f"{self.github_client.user}/{repo_name}"
            repo_object = self.foreign_repos.get(repo_name) or self.user_repos.get(
                repo_name
            )

            if not repo_object:
                owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
                query_data = {
                    "query": GithubQuery.REPO_QUERY.value,
                    "variables": {"owner": owner, "repositoryName": repo},
                }
                response = await self.github_client.api_call(query_data=query_data)
                repo_object = response.get("data", {}).get(  # pyright: ignore
                    REPOSITORY_OBJECT
                )
            repo_object.update(
                {
                    "_id": repo_object.pop("id"),
                    "_timestamp": repo_object.pop("updatedAt"),
                    "type": ObjectType.REPOSITORY.value,
                }
            )
            yield repo_object

    async def _fetch_repos(self):
        try:
            if self.github_client.user is None:
                self.github_client.user = await self.github_client.get_logged_in_user()

            if self.github_client.repos == [WILDCARD]:
                async for repo_object in self._get_personal_repos():
                    yield repo_object
            else:
                async for repo_object in self._get_configured_repos():
                    yield repo_object
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the repository. Exception: {exception}"
            )

    async def _fetch_remaining_data(
        self, variables, object_type, query, field_type, keys
    ):
        async for response in self.github_client.paginated_api_call(
            variables=variables, query=query, keys=keys
        ):
            yield response.get("data", {}).get(  # pyright: ignore
                REPOSITORY_OBJECT, {}
            ).get(object_type, {}).get(field_type, {}).get("nodes")

    async def _fetch_remaining_fields(
        self, type_obj, object_type, owner, repo, field_type
    ):
        sample_dict = {
            "reviews": {
                "query": GithubQuery.REVIEW_QUERY.value,
                "es_field": "reviews_comments",
            },
            "reviewRequests": {
                "query": GithubQuery.REVIEWERS_QUERY.value,
                "es_field": "requested_reviewers",
            },
            "comments": {
                "query": GithubQuery.COMMENT_QUERY.value.format(
                    object_type=object_type
                ),
                "es_field": "issue_comments",
            },
            "labels": {
                "query": GithubQuery.LABELS_QUERY.value.format(object_type=object_type),
                "es_field": "labels_field",
            },
            "assignees": {
                "query": GithubQuery.ASSIGNEES_QUERY.value.format(
                    object_type=object_type
                ),
                "es_field": "assignees_list",
            },
        }
        page_info = type_obj.get(field_type, {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            variables = {
                "owner": owner,
                "name": repo,
                "number": type_obj.get("number"),
                "cursor": page_info.get("endCursor"),
            }
            async for response in self._fetch_remaining_data(
                variables=variables,
                object_type=object_type,
                query=sample_dict[field_type]["query"],
                field_type=field_type,
                keys=[REPOSITORY_OBJECT, object_type, field_type],
            ):
                if field_type == "reviews":
                    for review in response:
                        type_obj["reviews_comments"].append(
                            self._prepare_review_doc(review=review)
                        )
                else:
                    type_obj[sample_dict[field_type]["es_field"]].extend(response)

    async def _extract_pull_request(self, pull_request, owner, repo):
        reviews = [
            self._prepare_review_doc(review=review)
            for review in pull_request.get("reviews", {}).get("nodes")
        ]
        pull_request.update(
            self._prepare_pull_request_doc(pull_request=pull_request, reviews=reviews)
        )
        for field in ["comments", "reviewRequests", "labels", "assignees", "reviews"]:
            await self._fetch_remaining_fields(
                type_obj=pull_request,
                object_type=PULL_REQUEST_OBJECT,
                owner=owner,
                repo=repo,
                field_type=field,
            )
            pull_request.pop(field)
        yield pull_request

    async def _fetch_pull_requests(self, repo_name):
        try:
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            pull_request_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
            }
            async for response in self.github_client.paginated_api_call(
                variables=pull_request_variables,
                query=GithubQuery.PULL_REQUEST_QUERY.value,
                keys=[REPOSITORY_OBJECT, "pullRequests"],
            ):
                for pull_request in (
                    response.get("data", {})  # pyright: ignore
                    .get(REPOSITORY_OBJECT, {})
                    .get("pullRequests", {})
                    .get("nodes")
                ):
                    async for pull_request_doc in self._extract_pull_request(
                        pull_request=pull_request, owner=owner, repo=repo
                    ):
                        yield pull_request_doc
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the pull requests. Exception: {exception}"
            )

    async def _extract_issues(self, response, owner, repo):
        for issue in (
            response.get("data", {})
            .get(REPOSITORY_OBJECT, {})
            .get("issues", {})
            .get("nodes")
        ):
            issue.update(self._prepare_issue_doc(issue=issue))
            for field in ["comments", "labels", "assignees"]:
                await self._fetch_remaining_fields(
                    type_obj=issue,
                    object_type="issue",
                    owner=owner,
                    repo=repo,
                    field_type=field,
                )
                issue.pop(field)
            yield issue

    async def _fetch_issues(self, repo_name):
        try:
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            issue_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
            }
            async for response in self.github_client.paginated_api_call(
                variables=issue_variables,
                query=GithubQuery.ISSUE_QUERY.value,
                keys=[REPOSITORY_OBJECT, "issues"],
            ):
                async for issue in self._extract_issues(
                    response=response, owner=owner, repo=repo
                ):
                    yield issue
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the issues. Exception: {exception}"
            )

    async def _fetch_last_commit_timestamp(self, repo_name, path):
        commit, *_ = await self.github_client.get_response(  # pyright: ignore
            resource=self.github_client.endpoints["COMMITS"].format(
                repo_name=repo_name, path=path
            )
        )
        return commit["commit"]["committer"]["date"]

    async def _fetch_files(self, repo_name, default_branch):
        try:
            file_tree = await self.github_client.get_response(
                resource=self.github_client.endpoints["TREE"].format(
                    repo_name=repo_name, default_branch=default_branch
                )
            )

            for repo_object in file_tree.get("tree", []):
                last_commit_timestamp = await self._fetch_last_commit_timestamp(
                    repo_name=repo_name, path=repo_object["path"]
                )
                if repo_object["type"] == BLOB:
                    file_name = repo_object["path"].split("/")[-1]
                    file_extension = (
                        file_name[file_name.rfind(".") :]  # noqa
                        if "." in file_name
                        else ""
                    )
                    repo_object.update(
                        {
                            "_timestamp": last_commit_timestamp,
                            "repo_name": repo_name,
                            "name": file_name,
                            "extension": file_extension,
                        }
                    )

                    document = self.adapt_gh_doc_to_es_doc(
                        github_document=repo_object, schema=FILE_SCHEMA
                    )

                    document["_id"] = f"{repo_name}/{repo_object['path']}"
                    yield document, repo_object
                else:
                    repo_object.update(
                        {
                            "_id": f"{repo_name}/{repo_object['path']}",
                            "_timestamp": last_commit_timestamp,
                        }
                    )
                    yield repo_object, None
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}"
            )

    async def _get_document_with_content(self, url, attachment_name, document):
        file_data = await self.github_client.get_response(resource=url)
        temp_filename = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            await async_buffer.write(
                decode_base64_value(content=file_data["content"])  # pyright: ignore
            )
            temp_filename = str(async_buffer.name)

        self._logger.debug(f"Calling convert_to_b64 for file : {attachment_name}")
        await asyncio.to_thread(convert_to_b64, source=temp_filename)
        async with aiofiles.open(file=temp_filename, mode="r") as async_buffer:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await async_buffer.read()).strip()

        try:
            await remove(temp_filename)
        except Exception as exception:
            self._logger.warning(
                f"Could not remove file from: {temp_filename}. Error: {exception}"
            )
        return document

    def _pre_checks_for_get_content(
        self, attachment_extension, attachment_name, attachment_size
    ):
        if attachment_extension == "":
            self._logger.warning(
                f"Files without extension are not supported by TIKA, skipping {attachment_name}."
            )
            return

        elif attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        return True

    async def get_content(self, attachment, timestamp=None, doit=False):
        """Extracts the content for Apache TIKA supported file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        attachment_size = int(attachment["size"])
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment["name"]
        attachment_extension = attachment["extension"]

        if not self._pre_checks_for_get_content(
            attachment_extension=attachment_extension,
            attachment_name=attachment_name,
            attachment_size=attachment_size,
        ):
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": f"{attachment['repo_name']}/{attachment['name']}",
            "_timestamp": attachment["_timestamp"],
        }

        return await self._get_document_with_content(
            url=attachment["url"],
            attachment_name=attachment_name,
            document=document,
        )

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GitHub objects in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GitHub.
        """
        async for repo in self._fetch_repos():
            yield repo, None
            repo_name = repo.get("nameWithOwner")
            default_branch = (
                repo.get("defaultBranchRef", {}).get("name")
                if repo.get("defaultBranchRef")
                else None
            )

            async for pull_request in self._fetch_pull_requests(repo_name=repo_name):
                yield pull_request, None

            async for issue in self._fetch_issues(repo_name=repo_name):
                yield issue, None

            if default_branch:
                async for file_document, attachment_metadata in self._fetch_files(
                    repo_name=repo_name, default_branch=default_branch
                ):
                    if file_document["type"] == BLOB:
                        yield file_document, partial(
                            self.get_content, attachment=attachment_metadata
                        )
                    else:
                        yield file_document, None
