#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from collections import defaultdict
from functools import partial

from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import nested_get_from_dict

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.sources.github.client import GitHubClient
from connectors.sources.github.query import GithubQuery
from connectors.sources.github.utils import (
    BLOB,
    FILE,
    FILE_SCHEMA,
    GITHUB_APP,
    GITHUB_CLOUD,
    GITHUB_SERVER,
    NODE_SIZE,
    PATH_SCHEMA,
    PERSONAL_ACCESS_TOKEN,
    PULL_REQUEST_OBJECT,
    REPOSITORY_OBJECT,
    RETRIES,
    SUPPORTED_EXTENSION,
    WILDCARD,
    ForbiddenException,
    ObjectType,
    UnauthorizedException,
)
from connectors.sources.github.validator import GitHubAdvancedRulesValidator
from connectors.utils import (
    HTTPS_URL_PATTERN,
    decode_base64_value,
)


class GitHubDataSource(BaseDataSource):
    """GitHub"""

    name = "GitHub"
    service_type = "github"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the GitHub instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.github_client = GitHubClient(
            auth_method=self.configuration["auth_method"],
            base_url="https://api.github.com"
            if self.configuration["data_source"] == GITHUB_CLOUD
            else f"{self.configuration['host'].rstrip('/')}/api",
            app_id=self.configuration["app_id"],
            private_key=self.configuration["private_key"],
            token=self.configuration["token"],
            ssl_enabled=self.configuration["ssl_enabled"],
            ssl_ca=self.configuration["ssl_ca"],
        )
        self.configured_repos = self.configuration["repositories"]
        self.user_repos = {}
        self.org_repos = {}
        self.foreign_repos = {}
        self.prev_repos = []
        self.members = {}
        self._user = None
        # A dict caches GitHub App installation info, where the key is the org name/user login,
        # and the value is the installation id
        self._installations = {}

    def _set_internal_logger(self):
        self.github_client.set_logger(self._logger)

    def advanced_rules_validators(self):
        return [GitHubAdvancedRulesValidator(self)]

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GitHub.

        Returns:
            dict: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Data source",
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
                "label": "Server URL",
                "order": 2,
                "type": "str",
                "validations": [{"type": "regex", "constraint": HTTPS_URL_PATTERN}],
            },
            "auth_method": {
                "display": "dropdown",
                "label": "Authentication method",
                "options": [
                    {"label": "Personal access token", "value": PERSONAL_ACCESS_TOKEN},
                    {"label": "GitHub App", "value": GITHUB_APP},
                ],
                "order": 3,
                "type": "str",
                "value": PERSONAL_ACCESS_TOKEN,
            },
            "token": {
                "depends_on": [
                    {"field": "auth_method", "value": PERSONAL_ACCESS_TOKEN}
                ],
                "label": "Token",
                "order": 4,
                "sensitive": True,
                "type": "str",
            },
            "repo_type": {
                "display": "dropdown",
                "label": "Repository Type",
                "options": [
                    {"label": "Organization", "value": "organization"},
                    {"label": "Other", "value": "other"},
                ],
                "order": 5,
                "tooltip": "The Document Level Security feature is not available for the Other Repository Type",
                "type": "str",
                "value": "other",
            },
            "org_name": {
                "depends_on": [
                    {"field": "auth_method", "value": PERSONAL_ACCESS_TOKEN},
                    {"field": "repo_type", "value": "organization"},
                ],
                "label": "Organization Name",
                "order": 6,
                "type": "str",
            },
            "app_id": {
                "depends_on": [{"field": "auth_method", "value": GITHUB_APP}],
                "display": "numeric",
                "label": "App ID",
                "order": 7,
                "type": "int",
            },
            "private_key": {
                "depends_on": [{"field": "auth_method", "value": GITHUB_APP}],
                "display": "textarea",
                "label": "App private key",
                "order": 8,
                "sensitive": True,
                "type": "str",
            },
            "repositories": {
                "display": "textarea",
                "label": "List of repositories",
                "order": 9,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 10,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 11,
                "type": "str",
            },
            "retry_count": {
                "display_value": RETRIES,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 12,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": RETRIES,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 13,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "depends_on": [{"field": "repo_type", "value": "organization"}],
                "label": "Enable document level security",
                "order": 14,
                "tooltip": "Document level security ensures identities and permissions set in GitHub are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return (
            self.configuration["repo_type"] == "organization"
            and self.configuration["use_document_level_security"]
        )

    async def _logged_in_user(self):
        if self.configuration["auth_method"] != PERSONAL_ACCESS_TOKEN:
            return None
        if self._user:
            return self._user
        self._user = await self.github_client.get_logged_in_user()
        return self._user

    async def get_invalid_repos(self):
        self._logger.debug(
            "Checking if there are any inaccessible repositories configured"
        )
        if self.configuration["auth_method"] == GITHUB_APP:
            return await self._get_invalid_repos_for_github_app()
        else:
            return await self._get_invalid_repos_for_personal_access_token()

    async def _get_invalid_repos_for_github_app(self) -> list[str]:
        # A github app can be installed on multiple orgs/personal accounts,
        # so the repo must be configured in the format of 'OWNER/REPO', any other format will be rejected
        invalid_repos = set(
            filter(
                lambda repo: repo
                and repo.strip()
                and len(repo.strip().split("/")) != 2,
                self.configured_repos,
            )
        )

        # Group valid repos by owner for batch validation
        valid_repos = [
            repo for repo in self.configured_repos if repo not in invalid_repos
        ]
        repos_by_owner = defaultdict(list)

        await self._fetch_installations()

        for full_repo_name in valid_repos:
            owner, repo_name = self.github_client.get_repo_details(
                repo_name=full_repo_name
            )

            # Check if GitHub App is installed on this owner
            if owner not in self._installations:
                self._logger.debug(
                    f"Invalid repo {full_repo_name} as the github app is not installed on {owner}"
                )
                invalid_repos.add(full_repo_name)
                continue

            repos_by_owner[owner].append(full_repo_name)

        # Batch validate repos for each owner
        for owner, owner_repos in repos_by_owner.items():
            await self.github_client.update_installation_id(self._installations[owner])

            batch_results = (
                await self.github_client.get_repos_by_fully_qualified_name_batch(
                    owner_repos
                )
            )

            for repo_name, repo_data in batch_results.items():
                if repo_data:
                    if self.configuration["repo_type"] == "organization":
                        if owner not in self.org_repos:
                            self.org_repos[owner] = {}
                        self.org_repos[owner][repo_name] = repo_data
                    else:
                        if owner not in self.user_repos:
                            self.user_repos[owner] = {}
                        self.user_repos[owner][repo_name] = repo_data
                else:
                    self._logger.debug(f"Detected invalid repository: {repo_name}.")
                    invalid_repos.add(repo_name)

        return list(invalid_repos)

    async def _get_repo_object_for_github_app(
        self, owner: str, repo_name: str
    ) -> dict | None:
        # Note: this method fetches potentially all user or org repos and caches them,
        # so it should be used sparingly as it could consume a lot of API rate limit
        # due to possibly multiple pages of repos
        await self._fetch_installations()
        full_repo_name = f"{owner}/{repo_name}"
        if owner not in self._installations:
            self._logger.debug(
                f"Invalid repo {full_repo_name} as the github app is not installed on {owner}"
            )
            return None
        await self.github_client.update_installation_id(self._installations[owner])

        if self.configuration["repo_type"] == "organization":
            if owner not in self.org_repos:
                # get repos for an org and cached it in self.org_repos
                async for _ in self._get_org_repos(owner):
                    pass

            cached_repo = self.org_repos
        else:
            if owner not in self.user_repos:
                # get repos for a user can cached it in self.user_repos
                async for _ in self._get_personal_repos(owner):
                    pass

            cached_repo = self.user_repos

        if full_repo_name not in cached_repo[owner]:
            self._logger.debug(
                f"Invalid repo {full_repo_name} as it's either non-existing or not accessible"
            )
            return None

        return cached_repo[owner][full_repo_name]

    async def _get_invalid_repos_for_personal_access_token(self) -> list[str]:
        try:
            all_repos: list[str] = []
            # Combine all repos for unified batch validation
            if self.configuration["repo_type"] == "other":
                logged_in_user = await self._logged_in_user()
                foreign_repos, configured_repos = self.github_client.bifurcate_repos(
                    repos=self.configured_repos,
                    owner=logged_in_user,
                )
                all_repos = configured_repos + foreign_repos
            else:
                foreign_repos, configured_repos = self.github_client.bifurcate_repos(
                    repos=self.configured_repos,
                    owner=self.configuration["org_name"],
                )
                all_repos = configured_repos + foreign_repos

            invalid_repos = []
            batch_results = (
                await self.github_client.get_repos_by_fully_qualified_name_batch(
                    all_repos
                )
            )
            for repo_name, repo_data in batch_results.items():
                if not repo_data:
                    self._logger.debug(f"Detected invalid repository: {repo_name}.")
                    invalid_repos.append(repo_name)
                    continue
                # Store valid repos for potential later use
                if repo_name in foreign_repos:
                    self.foreign_repos[repo_name] = repo_data
                else:
                    # Store configured repos in the appropriate cache
                    if self.configuration["repo_type"] == "other":
                        logged_in_user = await self._logged_in_user()
                        self.user_repos.setdefault(logged_in_user, {})[repo_name] = (
                            repo_data
                        )
                    else:
                        org_name = self.configuration["org_name"]
                        self.org_repos.setdefault(org_name, {})[repo_name] = repo_data
            return invalid_repos
        except Exception as exception:
            self._logger.exception(
                f"Error while checking for inaccessible repositories. Exception: {exception}.",
                exc_info=True,
            )
            raise

    async def _user_access_control_doc(self, user):
        user_id = user.get("id", "")
        user_name = user.get("login", "")
        user_email = user.get("email", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_user_name = _prefix_username(user=user_name)
        _prefixed_email = _prefix_email(email=user_email)
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "user_name": _prefixed_user_name,
                "email": _prefixed_email,
            },
            "created_at": user.get("updatedAt"),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_user_name, _prefixed_email]
        )

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for org_name in self._get_owners():
            async for user in self.github_client._fetch_all_members(org_name):
                yield await self._user_access_control_doc(user=user)

    async def _get_owners(self):
        """Yields organization or personal accounts based on the configured repo_type"""
        if self.configuration["auth_method"] == GITHUB_APP:
            await self._fetch_installations()
            for owner, installation_id in self._installations.items():
                await self.github_client.update_installation_id(installation_id)
                yield owner
        else:
            if self.configuration["repo_type"] == "organization":
                yield self.configuration["org_name"]
            else:
                yield await self._logged_in_user()

    async def _remote_validation(self):
        """Validate scope of the configured personal access token and accessibility of repositories

        Raises:
            ConfigurableFieldValueError: Insufficient privileges error.
        """

        await self._validate_personal_access_token_scopes()
        await self._validate_configured_repos()

    async def _validate_personal_access_token_scopes(self):
        if self.configuration["auth_method"] != PERSONAL_ACCESS_TOKEN:
            return

        scopes = await self.github_client.get_personal_access_token_scopes() or set()
        required_scopes = {"repo", "user", "read:org"}

        for scope in ["write:org", "admin:org"]:
            if scope in scopes:
                scopes.add("read:org")

        if required_scopes.issubset(scopes):
            extra_scopes = scopes - required_scopes
            if extra_scopes:
                self._logger.warning(
                    "The provided token has higher privileges than required. It is advisable to run the connector with least privielged token. Required scopes are 'repo', 'user', and 'read:org'."
                )
        else:
            msg = "Configured token does not have required rights to fetch the content. Required scopes are 'repo', 'user', and 'read:org'."
            raise ConfigurableFieldValueError(msg)

    async def _validate_configured_repos(self):
        if WILDCARD in self.configured_repos:
            return

        invalid_repos = await self.get_invalid_repos()
        if invalid_repos:
            msg = f"Inaccessible repositories '{', '.join(invalid_repos)}'."
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured repositories are accessible or not and scope of the token
        """
        await super().validate_config()
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
        return {
            es_field: github_document[github_field]
            for es_field, github_field in schema.items()
        }

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
        # review.author can be None if the user was deleted, so need to be extra null-safe
        author = review.get("author", {}) or {}

        return {
            "author": author.get("login"),
            "body": review.get("body"),
            "state": review.get("state"),
            "comments": review.get("comments", {}).get("nodes"),
        }

    async def _fetch_installations(self):
        """Fetches GitHub App installations, and populates instance variable self._installations
        Only populates Organization installations when repo_type is organization, and only populates User installations when repo_type is other
        """
        if self.configuration["auth_method"] != GITHUB_APP:
            return {}
        if self._installations:
            return self._installations

        async for installation in self.github_client.get_installations():
            if (
                self.configuration["repo_type"] == "organization"
                and installation["account"]["type"] == "Organization"
            ) or (
                self.configuration["repo_type"] == "other"
                and installation["account"]["type"] == "User"
            ):
                self._installations[installation["account"]["login"]] = installation[
                    "id"
                ]

        return self._installations

    async def _get_personal_repos(self, user):
        # Note: this method fetches potentially all user repos and caches them,
        # so it should be used sparingly as it could consume a lot of API rate limit
        # due to possibly multiple pages of repos
        self._logger.info(f"Fetching personal repos {user}")
        if user in self.user_repos:
            for repo_object in self.user_repos[user]:
                yield repo_object
        else:
            self.user_repos[user] = {}
            async for repo_object in self.github_client.get_user_repos(user):
                self.user_repos[user][repo_object["nameWithOwner"]] = repo_object
                yield repo_object

    async def _get_org_repos(self, org_name):
        # Note: this method fetches potentially all org repos and caches them,
        # so it should be used sparingly as it could consume a lot of API rate limit
        # due to possibly multiple pages of repos
        self._logger.info(f"Fetching org repos for {org_name}")
        if org_name in self.org_repos:
            for repo_object in self.org_repos[org_name]:
                yield repo_object
        else:
            self.org_repos[org_name] = {}
            async for repo_object in self.github_client.get_org_repos(org_name):
                self.org_repos[org_name][repo_object["nameWithOwner"]] = repo_object
                yield repo_object

    async def _get_configured_repos(self, configured_repos):
        # Note: this method calls _get_repo_object_for_github_app - see comments there
        # for potential performance implications
        self._logger.info(f"Fetching configured repos: '{configured_repos}'")
        for repo_name in configured_repos:
            self._logger.info(f"Fetching repo: '{repo_name}'")
            if repo_name in ["", None]:
                continue

            if self.configuration["auth_method"] == PERSONAL_ACCESS_TOKEN:
                # Converting the local repository names to username/repo_name format.
                logged_in_user = await self._logged_in_user()
                if "/" not in repo_name:
                    owner = (
                        logged_in_user
                        if self.configuration["repo_type"] == "other"
                        else self.configuration["org_name"]
                    )
                    repo_name = f"{owner}/{repo_name}"
                repo_object = self.foreign_repos.get(repo_name) or self.user_repos.get(
                    logged_in_user, {}
                ).get(repo_name)

                if not repo_object:
                    owner, repo = self.github_client.get_repo_details(
                        repo_name=repo_name
                    )
                    variables = {"owner": owner, "repositoryName": repo}
                    data = await self.github_client.graphql(
                        query=GithubQuery.REPO_QUERY.value, variables=variables
                    )
                    repo_object = data.get(REPOSITORY_OBJECT)
            else:
                owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
                repo_object = await self._get_repo_object_for_github_app(owner, repo)
                # update installation and re-generate installation access token
                await self.github_client.update_installation_id(
                    self._installations[owner]
                )
            yield self._convert_repo_object_to_doc(repo_object)

    async def _fetch_repos(self):
        # Note: this method calls _get_configured_repos
        # which in turn calls _get_repo_object_for_github_app
        # or _get_personal_repos/_get_org_repos, see comments there
        # for potential performance/rate limit implications
        self._logger.info("Fetching repos")
        try:
            if (
                WILDCARD in self.configured_repos
                and self.configuration["repo_type"] == "other"
            ):
                async for user in self._get_owners():
                    async for repo_object in self._get_personal_repos(user):
                        yield self._convert_repo_object_to_doc(repo_object)
            elif (
                WILDCARD in self.configured_repos
                and self.configuration["repo_type"] == "organization"
            ):
                async for org in self._get_owners():
                    async for repo_object in self._get_org_repos(org):
                        yield self._convert_repo_object_to_doc(repo_object)
            else:
                async for repo_object in self._get_configured_repos(
                    configured_repos=self.configured_repos
                ):
                    yield repo_object
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the repository. Exception: {exception}",
                exc_info=True,
            )

    def _convert_repo_object_to_doc(self, repo_object):
        repo_object = repo_object.copy()
        repo_object.update(
            {
                "_id": repo_object.pop("id"),
                "_timestamp": repo_object.pop("updatedAt"),
                "type": ObjectType.REPOSITORY.value,
            }
        )
        return repo_object

    async def _fetch_remaining_data(
        self, variables, object_type, query, field_type, keys
    ):
        async for response in self.github_client.paginated_api_call(
            query=query, variables=variables, keys=keys
        ):
            yield nested_get_from_dict(
                response, [REPOSITORY_OBJECT, object_type, field_type, "nodes"]
            )

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
                    object_type=object_type, node_size=NODE_SIZE
                ),
                "es_field": "issue_comments",
            },
            "labels": {
                "query": GithubQuery.LABELS_QUERY.value.format(
                    object_type=object_type, node_size=NODE_SIZE
                ),
                "es_field": "labels_field",
            },
            "assignees": {
                "query": GithubQuery.ASSIGNEES_QUERY.value.format(
                    object_type=object_type, node_size=NODE_SIZE
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
                    for review in response:  # pyright: ignore
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

    async def _fetch_pull_requests(
        self,
        repo_name,
        response_key,
        filter_query=None,
    ):
        self._logger.info(
            f"Fetching pull requests from '{repo_name}' with response_key '{response_key}' and filter query: '{filter_query}'"
        )
        try:
            query = (
                GithubQuery.SEARCH_QUERY.value
                if filter_query
                else GithubQuery.PULL_REQUEST_QUERY.value
            )
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            pull_request_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
                "filter_query": filter_query,
            }
            async for response in self.github_client.paginated_api_call(
                query=query,
                variables=pull_request_variables,
                keys=response_key,
            ):
                for pull_request in nested_get_from_dict(  # pyright: ignore
                    response, response_key + ["nodes"], default=[]
                ):
                    async for pull_request_doc in self._extract_pull_request(
                        pull_request=pull_request, owner=owner, repo=repo
                    ):
                        yield pull_request_doc
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the pull requests. Exception: {exception}",
                exc_info=True,
            )

    async def _extract_issues(self, response, owner, repo, response_key):
        for issue in nested_get_from_dict(  # pyright: ignore
            response, response_key + ["nodes"], default=[]
        ):
            issue.update(self._prepare_issue_doc(issue=issue))
            for field in ["comments", "labels", "assignees"]:
                await self._fetch_remaining_fields(
                    type_obj=issue,
                    object_type=ObjectType.ISSUE.value.lower(),
                    owner=owner,
                    repo=repo,
                    field_type=field,
                )
                issue.pop(field)
            yield issue

    async def _fetch_issues(
        self,
        repo_name,
        response_key,
        filter_query=None,
    ):
        self._logger.info(
            f"Fetching issues from repo: {repo_name} with response_key: '{response_key}' and filter_query: '{filter_query}'"
        )
        try:
            query = (
                GithubQuery.SEARCH_QUERY.value
                if filter_query
                else GithubQuery.ISSUE_QUERY.value
            )
            owner, repo = self.github_client.get_repo_details(repo_name=repo_name)
            issue_variables = {
                "owner": owner,
                "name": repo,
                "cursor": None,
                "filter_query": filter_query,
            }
            async for response in self.github_client.paginated_api_call(
                query=query,
                variables=issue_variables,
                keys=response_key,
            ):
                async for issue in self._extract_issues(
                    response=response, owner=owner, repo=repo, response_key=response_key
                ):
                    yield issue
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the issues. Exception: {exception}",
                exc_info=True,
            )

    async def _fetch_last_commit_timestamp(self, repo_name, path):
        commit, *_ = await self.github_client.get_github_item(  # pyright: ignore
            resource=self.github_client.endpoints["COMMITS"].format(
                repo_name=repo_name, path=path
            )
        )
        return commit["commit"]["committer"]["date"]

    async def _format_file_document(self, repo_object, repo_name, schema):
        file_name = repo_object["path"].split("/")[-1]
        file_extension = (
            file_name[file_name.rfind(".") :] if "." in file_name else ""  # noqa
        )
        if file_extension.lower() in SUPPORTED_EXTENSION:
            last_commit_timestamp = await self._fetch_last_commit_timestamp(
                repo_name=repo_name, path=repo_object["path"]
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
                github_document=repo_object, schema=schema
            )

            document["_id"] = f"{repo_name}/{repo_object['path']}"
            return document, repo_object

    async def _fetch_files(self, repo_name, default_branch):
        self._logger.info(
            f"Fetching files from repo: '{repo_name}' (branch: '{default_branch}')"
        )
        try:
            file_tree = await self.github_client.get_github_item(
                resource=self.github_client.endpoints["TREE"].format(
                    repo_name=repo_name, default_branch=default_branch
                )
            )

            for repo_object in file_tree.get("tree", []):
                if repo_object["type"] == BLOB:
                    if document := await self._format_file_document(
                        repo_object=repo_object, repo_name=repo_name, schema=FILE_SCHEMA
                    ):
                        yield document
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}",
                exc_info=True,
            )

    async def _fetch_files_by_path(self, repo_name, path):
        self._logger.info(f"Fetching files from repo: '{repo_name}' (path: '{path}')")
        try:
            for repo_object in await self.github_client.get_github_item(
                resource=self.github_client.endpoints["PATH"].format(
                    repo_name=repo_name, path=path
                )
            ):  # pyright: ignore
                if repo_object["type"] == FILE:
                    if document := await self._format_file_document(
                        repo_object=repo_object,
                        repo_name=repo_name,
                        schema=PATH_SCHEMA,
                    ):
                        yield document
        except UnauthorizedException:
            raise
        except ForbiddenException:
            raise
        except Exception as exception:
            self._logger.warning(
                f"Something went wrong while fetching the files of {repo_name}. Exception: {exception}",
                exc_info=True,
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
        file_size = int(attachment["size"])
        if not (doit and file_size > 0):
            return

        filename = attachment["name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": f"{attachment['repo_name']}/{filename}",
            "_timestamp": attachment["_timestamp"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.download_func,
                attachment["url"],
            ),
        )

    async def download_func(self, url):
        file_data = await self.github_client.get_github_item(resource=url)
        if file_data:
            yield decode_base64_value(content=file_data["content"])
        else:
            yield

    def _filter_rule_query(self, repo, query, query_type):
        """
        Filters a query based on the query type.

        Args:
            repo (str): Query repo name.
            query (str): The input query.
            query_type (str): The type of query ("pr" or "issue").

        Returns:
            tuple: A tuple containing a boolean value indicating whether the query should be included or excluded and the modified query.
        """
        if query_type == ObjectType.PR.value:
            if "is:issue" in query:
                return False, query
            elif "is:pr" in query:
                return True, f"repo:{repo} {query}"
            return True, f"repo:{repo} is:pr {query}"
        elif query_type == ObjectType.ISSUE.value.lower():
            if "is:pr" in query:
                return False, query
            elif "is:issue" in query:
                return True, f"repo:{repo} {query}"
            return True, f"repo:{repo} is:issue {query}"
        else:
            return False, query

    def is_previous_repo(self, repo_name):
        if repo_name in self.prev_repos:
            return True
        self.prev_repos.append(repo_name)
        return False

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _fetch_access_control(self, repo_name):
        owner, repo = self.github_client.get_repo_details(repo_name)
        collaborator_variables = {
            "orgName": owner,
            "repoName": repo,
            "cursor": None,
        }
        access_control = []
        if owner not in self.members:
            members = set()
            async for user in self.github_client._fetch_all_members(owner):
                members.add(user.get("id"))
            self.members[owner] = members
        async for response in self.github_client.paginated_api_call(
            query=GithubQuery.COLLABORATORS_QUERY.value,
            variables=collaborator_variables,
            keys=["repository", "collaborators"],
        ):
            for user in nested_get_from_dict(  # pyright: ignore
                response, ["repository", "collaborators", "edges"], default=[]
            ):
                user_id = user.get("node", {}).get("id")
                user_name = user.get("node", {}).get("login")
                user_email = user.get("node", {}).get("email")
                if user_id in self.members[owner]:
                    access_control.append(_prefix_user_id(user_id=user_id))
                    if user_name:
                        access_control.append(_prefix_username(user=user_name))
                    if user_email:
                        access_control.append(_prefix_email(email=user_email))
        return access_control

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GitHub objects in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GitHub.
        """
        if filtering and filtering.has_advanced_rules():
            self._logger.info("Using advanced rules")
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                repo = await anext(
                    self._get_configured_repos(configured_repos=[rule["repository"]])
                )
                yield repo, None
                repo_name = repo.get("nameWithOwner")

                if pull_request_query := rule["filter"].get(ObjectType.PR.value):
                    query_status, pull_request_query = self._filter_rule_query(
                        repo=repo_name,
                        query=pull_request_query,
                        query_type=ObjectType.PR.value,
                    )
                    if query_status:
                        async for pull_request in self._fetch_pull_requests(
                            repo_name=repo_name,
                            response_key=["search"],
                            filter_query=pull_request_query,
                        ):
                            yield pull_request, None
                    else:
                        self._logger.warning(
                            f"Skipping pr for rule: {pull_request_query}"
                        )

                if issue_query := rule["filter"].get(ObjectType.ISSUE.value.lower()):
                    query_status, issue_query = self._filter_rule_query(
                        repo=repo_name,
                        query=issue_query,
                        query_type=ObjectType.ISSUE.value.lower(),
                    )
                    if query_status:
                        async for issue in self._fetch_issues(
                            repo_name=repo_name,
                            response_key=["search"],
                            filter_query=issue_query,
                        ):
                            yield issue, None
                    else:
                        self._logger.warning(f"Skipping issue for query: {issue_query}")

                if branch := rule["filter"].get(ObjectType.BRANCH.value):
                    async for file_document, attachment_metadata in self._fetch_files(
                        repo_name=repo_name, default_branch=branch
                    ):
                        if file_document["type"] == BLOB:
                            yield (
                                file_document,
                                partial(
                                    self.get_content, attachment=attachment_metadata
                                ),
                            )
                        else:
                            yield file_document, None

                if path := rule["filter"].get(ObjectType.PATH.value):
                    async for (
                        file_document,
                        attachment_metadata,
                    ) in self._fetch_files_by_path(repo_name=repo_name, path=path):
                        if file_document["type"] == FILE:
                            attachment_metadata["url"] = attachment_metadata["git_url"]
                            yield (
                                file_document,
                                partial(
                                    self.get_content, attachment=attachment_metadata
                                ),
                            )
                        else:
                            yield file_document, None
        else:
            async for repo in self._fetch_repos():
                if self.is_previous_repo(repo["nameWithOwner"]):
                    continue

                access_control = []
                is_public_repo = repo.get("visibility").lower() == "public"
                needs_access_control = self._dls_enabled() and not is_public_repo

                if needs_access_control:
                    access_control = await self._fetch_access_control(
                        repo_name=repo.get("nameWithOwner")
                    )

                if needs_access_control:
                    yield (
                        self._decorate_with_access_control(
                            document=repo, access_control=access_control
                        ),
                        None,
                    )
                else:
                    yield repo, None

                repo_name = repo.get("nameWithOwner")
                default_branch = (
                    repo.get("defaultBranchRef", {}).get("name")
                    if repo.get("defaultBranchRef")
                    else None
                )

                async for pull_request in self._fetch_pull_requests(
                    repo_name=repo_name,
                    response_key=[REPOSITORY_OBJECT, "pullRequests"],
                ):
                    if needs_access_control:
                        yield (
                            self._decorate_with_access_control(
                                document=pull_request, access_control=access_control
                            ),
                            None,
                        )
                    else:
                        yield pull_request, None

                async for issue in self._fetch_issues(
                    repo_name=repo_name, response_key=[REPOSITORY_OBJECT, "issues"]
                ):
                    if needs_access_control:
                        yield (
                            self._decorate_with_access_control(
                                document=issue, access_control=access_control
                            ),
                            None,
                        )
                    else:
                        yield issue, None

                if default_branch:
                    async for file_document, attachment_metadata in self._fetch_files(
                        repo_name=repo_name, default_branch=default_branch
                    ):
                        if file_document["type"] == BLOB:
                            if needs_access_control:
                                yield (
                                    self._decorate_with_access_control(
                                        document=file_document,
                                        access_control=access_control,
                                    ),
                                    partial(
                                        self.get_content, attachment=attachment_metadata
                                    ),
                                )
                            else:
                                yield (
                                    file_document,
                                    partial(
                                        self.get_content, attachment=attachment_metadata
                                    ),
                                )
                        else:
                            if needs_access_control:
                                yield (
                                    self._decorate_with_access_control(
                                        document=file_document,
                                        access_control=access_control,
                                    ),
                                    None,
                                )
                            else:
                                yield file_document, None


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)
