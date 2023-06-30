#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GitHub source module responsible to fetch documents from GitHub Cloud and Server."""
import asyncio
import json
import os
import time
from enum import Enum
from functools import cached_property, partial

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from gidgethub import BadRequest
from gidgethub.aiohttp import GitHubAPI

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    RetryStrategy,
    convert_to_b64,
    decode_base64_value,
    retryable,
)

WILDCARD = "*"
BLOB = "blob"

RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760  # ~ 10 Megabytes
PAGE_SIZE = 100
FORBIDDEN = 403

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.


class ObjectType(Enum):
    REPOSITORY = "Repository"
    ISSUE = "Issue"
    PR = "Pull request"


BASE_URL = "http://127.0.0.1:9091" if RUNNING_FTEST else "https://api.github.com"
USER_API = f"{BASE_URL}/user"

ENDPOINTS = {
    "ALL_REPOS": "/user/repos?type=all&per_page={page_size}",
    "REPO": "repos/{repo_name}",
    "USER": "/user",
    "PULLS": "/repos/{repo_name}/pulls?state=all&per_page={page_size}",
    "ISSUES": "/repos/{repo_name}/issues?state=all&per_page={page_size}",
    "TREE": "/repos/{repo_name}/git/trees/{default_branch}?recursive=1",
    "COMMITS": "repos/{repo_name}/commits?path={path}",
    "COMMENTS": "/repos/{repo_name}/issues/{number}/comments?per_page={page_size}",
    "REVIEW_COMMENTS": "/repos/{repo_name}/pulls/{number}/comments?per_page={page_size}",
    "REVIEWS": "/repos/{repo_name}/pulls/{number}/reviews?per_page={page_size}",
}

REPO_SCHEMA = {
    "_id": "id",
    "_timestamp": "updated_at",
    "name": "name",
    "full_name": "full_name",
    "html_url": "html_url",
    "description": "description",
    "visibility": "visibility",
    "language": "language",
    "default_branch": "default_branch",
    "fork": "fork",
    "open_issues": "open_issues",
    "forks_count": "forks_count",
    "watchers": "watchers",
    "stargazers_count": "stargazers_count",
    "created_at": "created_at",
}
ISSUE_SCHEMA = {
    "_id": "id",
    "_timestamp": "updated_at",
    "number": "number",
    "html_url": "html_url",
    "created_at": "created_at",
    "closed_at": "closed_at",
    "title": "title",
    "body": "body",
    "state": "state",
}
PULL_REQUEST_SCHEMA = {
    "_id": "id",
    "_timestamp": "updated_at",
    "number": "number",
    "html_url": "html_url",
    "created_at": "created_at",
    "closed_at": "closed_at",
    "title": "title",
    "body": "body",
    "state": "state",
    "merged_at": "merged_at",
    "merge_commit_sha": "merge_commit_sha",
}
FILE_SCHEMA = {
    "name": "name",
    "size": "size",
    "type": "type",
    "path": "path",
    "mode": "mode",
    "extension": "extension",
    "_timestamp": "_timestamp",
}


class UnauthorizedException(Exception):
    pass


class GitHubClient:
    def __init__(self, configuration):
        self.session = None
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.repos = self.configuration["repositories"]
        self.github_token = self.configuration["github_token"]
        self.headers = {
            "Authorization": f"Bearer {self.github_token}",
            "Accept": "application/vnd.github.raw",
        }

    def set_logger(self, logger_):
        self._logger = logger_

    def get_rate_limit_encountered(self, status_code, message):
        return status_code == FORBIDDEN and "rate limit" in str(message).lower()

    async def _get_retry_after(self):
        response = await self._get_client.getitem("/rate_limit")
        reset = response["resources"]["core"]["reset"]
        # Adding a 5 second delay to account for server delays
        return (reset - time.time()) + 5

    async def _put_to_sleep(self):
        retry_after = await self._get_retry_after()
        self._logger.debug(
            f"Connector will attempt to retry after {retry_after} seconds."
        )
        await self._sleeps.sleep(retry_after)
        raise Exception("Rate limit exceeded.")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_response(self, resource):
        """Execute request using getitem method of GitHubAPI.

        Args:
            resource (str): API to get the response

        Returns:
            dict/list: Response of the request
        """
        try:
            return await self._get_client.getitem(url=resource)
        except BadRequest as exception:
            if self.get_rate_limit_encountered(exception.status_code, exception):
                await self._put_to_sleep()
            elif self.is_unauthorized(exception=exception):
                raise UnauthorizedException(
                    "Your Github token is either expired or revoked. Please check again."
                ) from exception
            else:
                raise
        except Exception:
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_response_headers(self, method, url):
        """Execute request using _request method of GitHubAPI and returns response headers.

        Args:
            method (str): Method of the request
            url (str): API to get the response

        Returns:
            dict: Response of the headers
        """
        status_code, response_headers, data = await self._get_client._request(
            method=method, url=url, headers=self.headers
        )
        if status_code == 200:
            return response_headers
        try:
            data = data.decode("utf-8")
            message = json.loads(data)
        except ValueError as exception:
            self._logger.exception(
                f"Error while loading the response data. Error: {exception}"
            )
            raise
        if self.get_rate_limit_encountered(status_code, message):
            await self._put_to_sleep()
        else:
            raise Exception(
                f"Something went wrong while executing request Exception: {message['message']}"
            )

    async def get_paginated_response(self, resource):
        """Execute paginated request using getitem method of GithubAPI.

        Args:
            resource (str): API to get the response

        Yields:
            dict: Response of the request
        """
        page = 1
        while True:
            url = f"{resource}&page={page}"
            documents = await self.get_response(resource=url)
            if not documents:
                break
            for document in documents:
                yield document
            page += 1

    async def get_user_repos(self):
        async for response in self.get_paginated_response(
            resource=ENDPOINTS["ALL_REPOS"].format(page_size=PAGE_SIZE)
        ):
            yield response

    async def get_foreign_repo(self, repo_name):
        return await self.get_response(
            resource=ENDPOINTS["REPO"].format(repo_name=repo_name)
        )

    @cached_property
    def _get_client(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return GitHubAPI(
            self.session,
            requester="",
            oauth_token=self.github_token,
            base_url=BASE_URL,
        )

    async def ping(self):
        await self.get_response(resource=ENDPOINTS["USER"])

    async def close(self):
        self._sleeps.cancel()
        if self.session is not None:
            await self.session.close()
            del self._get_client

    def is_unauthorized(self, exception):
        return exception.status_code.value == 401


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
                "value": WILDCARD,
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

    async def get_invalid_repos(self):
        try:
            self._logger.debug(
                "Checking if there are any inaccessible repositories configured"
            )
            foreign_repos, configured_repos = [], []
            self.user = await self.github_client.get_response(
                resource=ENDPOINTS["USER"]
            )

            for repo_name in self.github_client.repos:
                if repo_name not in ["", None]:
                    if "/" in repo_name:
                        foreign_repos.append(repo_name)
                    else:
                        configured_repos.append(f"{self.user.get('login')}/{repo_name}")

            async for repo in self.github_client.get_user_repos():
                self.user_repos[repo["full_name"]] = repo
            invalid_repos = list(set(configured_repos) - set(self.user_repos.keys()))

            for repo_name in foreign_repos:
                try:
                    self.foreign_repos[
                        repo_name
                    ] = await self.github_client.get_foreign_repo(repo_name)
                except BadRequest:
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
        response_headers = await self.github_client.get_response_headers(
            method="get", url=USER_API
        )
        if "repo" not in response_headers.get("X-OAuth-Scopes"):  # pyright: ignore
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

    def _comment_doc(self, comment):
        return {
            "author": comment["user"]["login"],
            "body": comment["body"],
        }

    def _review_doc(self, review):
        return {
            "author": review["user"]["login"],
            "body": review["body"],
            "state": review["state"],
        }

    def _pull_request_doc(self, pull, repo_name, review_comments, comments, reviews):
        return {
            "type": ObjectType.PR.value,
            "owner": pull["user"]["login"],
            "repository": repo_name,
            "head_label": pull["head"]["label"],
            "base_label": pull["base"]["label"],
            "assignees": [assignee["login"] for assignee in pull["assignees"]],
            "requested_reviewers": [
                reviewer["login"] for reviewer in pull["requested_reviewers"]
            ],
            "requested_teams": [team["name"] for team in pull["requested_teams"]],
            "labels": [label["name"] for label in pull["labels"]],
            "review_comments": review_comments,
            "comments": comments,
            "reviews": reviews,
        }

    def _issue_doc(self, issue, repo_name, comments):
        return {
            "type": ObjectType.ISSUE.value,
            "owner": issue["user"]["login"],
            "repository": repo_name,
            "assignees": [assignee["login"] for assignee in issue["assignees"]],
            "labels": [label["name"] for label in issue["labels"]],
            "comments": comments,
        }

    async def fetch_repos(self):
        try:
            if self.github_client.repos == [WILDCARD]:
                if not self.user_repos:
                    async for repo in self.github_client.get_user_repos():
                        self.user_repos[repo["full_name"]] = repo
                for repo in self.user_repos.values():
                    document = self.adapt_gh_doc_to_es_doc(
                        github_document=repo, schema=REPO_SCHEMA
                    )
                    document["owner"] = repo["owner"]["login"]
                    document["type"] = ObjectType.REPOSITORY.value
                    yield document
            else:
                if self.user is None:
                    self.user = await self.github_client.get_response(
                        resource=ENDPOINTS["USER"]
                    )

                for repo_name in self.github_client.repos:
                    if repo_name in ["", None]:
                        continue

                    # Converting the local repository names to username/repo_name format.
                    if "/" not in repo_name:
                        repo_name = f"{self.user.get('login')}/{repo_name}"

                    repo_object = self.foreign_repos.get(
                        repo_name
                    ) or self.user_repos.get(repo_name)
                    if not repo_object:
                        repo_object = await self.github_client.get_response(
                            resource=ENDPOINTS["REPO"].format(repo_name=repo_name)
                        )

                    document = self.adapt_gh_doc_to_es_doc(
                        github_document=repo_object,
                        schema=REPO_SCHEMA,
                    )
                    document["owner"] = repo_object["owner"]["login"]  # pyright: ignore
                    document["type"] = ObjectType.REPOSITORY.value
                    yield document
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the repository. Exception: {exception}"
            )

    async def fetch_pull_requests(self, repo_name):
        try:
            async for pull in self.github_client.get_paginated_response(
                resource=ENDPOINTS["PULLS"].format(
                    repo_name=repo_name, page_size=PAGE_SIZE
                )
            ):
                document = self.adapt_gh_doc_to_es_doc(
                    github_document=pull, schema=PULL_REQUEST_SCHEMA
                )
                comments, review_comments, reviews = [], [], []
                async for review_comment in self.github_client.get_paginated_response(
                    ENDPOINTS["REVIEW_COMMENTS"].format(
                        repo_name=repo_name, number=pull["number"], page_size=PAGE_SIZE
                    )
                ):
                    review_comments.append(self._comment_doc(review_comment))
                async for comment in self.github_client.get_paginated_response(
                    ENDPOINTS["COMMENTS"].format(
                        repo_name=repo_name, number=pull["number"], page_size=PAGE_SIZE
                    )
                ):
                    comments.append(self._comment_doc(comment))
                async for review in self.github_client.get_paginated_response(
                    ENDPOINTS["REVIEWS"].format(
                        repo_name=repo_name, number=pull["number"], page_size=PAGE_SIZE
                    )
                ):
                    reviews.append(self._review_doc(review))
                document.update(
                    self._pull_request_doc(
                        pull, repo_name, review_comments, comments, reviews
                    )
                )
                yield document
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the pull requests of {repo_name}. Exception: {exception}"
            )

    async def fetch_issues(self, repo_name):
        try:
            async for issue in self.github_client.get_paginated_response(
                resource=ENDPOINTS["ISSUES"].format(
                    repo_name=repo_name, page_size=PAGE_SIZE
                )
            ):
                if issue.get("pull_request") is not None:
                    continue
                document = self.adapt_gh_doc_to_es_doc(
                    github_document=issue, schema=ISSUE_SCHEMA
                )
                comments = []
                async for comment in self.github_client.get_paginated_response(
                    resource=ENDPOINTS["COMMENTS"].format(
                        repo_name=repo_name, number=issue["number"], page_size=PAGE_SIZE
                    )
                ):
                    comments.append(self._comment_doc(comment))
                document.update(self._issue_doc(issue, repo_name, comments))
                yield document
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the issues of {repo_name}. Exception: {exception}"
            )

    async def fetch_last_commit_timestamp(self, repo_name, path):
        commits = await self.github_client.get_response(
            resource=ENDPOINTS["COMMITS"].format(repo_name=repo_name, path=path)
        )

        # Get last commit timstamp from the list of the commits
        return commits[0]["commit"]["committer"]["date"]  # pyright: ignore

    async def fetch_files(self, repo_name, default_branch):
        try:
            file_tree = await self.github_client.get_response(
                resource=ENDPOINTS["TREE"].format(
                    repo_name=repo_name, default_branch=default_branch
                )
            )

            for repo_object in file_tree.get("tree"):
                last_commit_timestamp = await self.fetch_last_commit_timestamp(
                    repo_name, repo_object["path"]
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
                    repo_object["_id"] = f"{repo_name}/{repo_object['path']}"
                    repo_object["_timestamp"] = last_commit_timestamp
                    yield repo_object, None
        except UnauthorizedException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}"
            )

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

        if attachment_extension == "":
            self._logger.warning(
                f"Files without extension are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"Files with the extension {attachment_extension} are not supported by TIKA, skipping {attachment_name}."
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": f"{attachment['repo_name']}/{attachment['name']}",
            "_timestamp": attachment["_timestamp"],
        }
        file_data = await self.github_client.get_response(resource=attachment["url"])
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

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GitHub objects in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GitHub.
        """
        async for repo in self.fetch_repos():
            yield repo, None
            repo_name = repo["full_name"]

            async for pull_request in self.fetch_pull_requests(repo_name=repo_name):
                yield pull_request, None

            async for issue in self.fetch_issues(repo_name=repo_name):
                yield issue, None

            async for file_document, attachment_metadata in self.fetch_files(
                repo_name=repo_name, default_branch=repo["default_branch"]
            ):
                if file_document["type"] == BLOB:
                    yield file_document, partial(
                        self.get_content, attachment=attachment_metadata
                    )
                else:
                    yield file_document, None
