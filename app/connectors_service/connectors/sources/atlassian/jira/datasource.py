#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Jira source module responsible to fetch documents from Jira on-prem or cloud server."""

import asyncio
from copy import copy
from datetime import datetime
from functools import partial
from urllib import parse

import pytz
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import (
    iso_utc,
)

from connectors.access_control import ACCESS_CONTROL
from connectors.sources.atlassian.jira.client import JiraClient
from connectors.sources.atlassian.jira.constants import (
    ATLASSIAN,
    ATTACHMENT_CLOUD,
    ATTACHMENT_SERVER,
    FINISHED,
    JIRA_CLOUD,
    JIRA_DATA_CENTER,
    JIRA_SERVER,
    MAX_CONCURRENCY,
    MAX_CONCURRENT_DOWNLOADS,
    PROJECT,
    QUEUE_MEM_SIZE,
    URLS,
    USER_QUERY,
    USERS,
    USERS_FOR_DATA_CENTER,
    WILDCARD,
)
from connectors.sources.atlassian.utils import (
    prefix_account_id,
    prefix_account_name,
    prefix_group_id,
)
from connectors.sources.atlassian.validator import (
    AtlassianAccessControl,
    AtlassianAdvancedRulesValidator,
)
from connectors.utils import (
    ConcurrentTasks,
    MemQueue,
)


class JiraDataSource(BaseDataSource):
    """Jira"""

    name = "Jira"
    service_type = "jira"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Jira

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.jira_client = JiraClient(configuration=configuration)
        self.atlassian_access_control = AtlassianAccessControl(self, self.jira_client)

        self.tasks = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

        self.project_permission_cache = {}
        self.custom_fields = {}

    def _set_internal_logger(self):
        self.jira_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Jira

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Jira data source",
                "options": [
                    {"label": "Jira Cloud", "value": JIRA_CLOUD},
                    {"label": "Jira Server", "value": JIRA_SERVER},
                    {"label": "Jira Data Center", "value": JIRA_DATA_CENTER},
                ],
                "order": 1,
                "type": "str",
                "value": JIRA_CLOUD,
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": JIRA_SERVER}],
                "label": "Jira Server username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": JIRA_SERVER}],
                "label": "Jira Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "data_center_username": {
                "depends_on": [{"field": "data_source", "value": JIRA_DATA_CENTER}],
                "label": "Jira Data Center username",
                "order": 4,
                "type": "str",
            },
            "data_center_password": {
                "depends_on": [{"field": "data_source", "value": JIRA_DATA_CENTER}],
                "label": "Jira Data Center password",
                "sensitive": True,
                "order": 5,
                "type": "str",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud email address",
                "order": 6,
                "type": "str",
                "tooltip": "Email address associated with Jira Cloud account. E.g. jane.doe@gmail.com",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud API token",
                "order": 7,
                "sensitive": True,
                "type": "str",
            },
            "jira_url": {
                "label": "Jira host url",
                "order": 8,
                "type": "str",
            },
            "projects": {
                "display": "textarea",
                "label": "Jira project keys",
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
                "default_value": 3,
                "display": "numeric",
                "label": "Retries for failed requests",
                "order": 12,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 13,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 14,
                "tooltip": "Document level security ensures identities and permissions set in Jira are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents. Only 1000 users can be fetched for Jira Data Center.",
                "type": "bool",
                "value": False,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 15,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
        }

    def _dls_enabled(self):
        """Check if document level security is enabled. This method checks whether document level security (DLS) is enabled based on the provided configuration.

        Returns:
            bool: True if document level security is enabled, False otherwise.
        """
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _project_access_control(self, project):
        if not self._dls_enabled():
            return []

        self._logger.info(
            f"Fetching users with read access to '{project['key']}' project"
        )
        access_control = set()
        async for actors in self.jira_client.user_information_list(
            key=f"projectKey={project['key']}"
        ):
            for actor in actors:
                if (
                    self.jira_client.data_source_type == JIRA_CLOUD
                    and actor.get("accountType", "") == ATLASSIAN
                ):
                    access_control.add(
                        prefix_account_id(account_id=actor.get("accountId"))
                    )
                    access_control.add(
                        prefix_account_name(account_name=actor.get("displayName"))
                    )
                elif self.jira_client.data_source_type in [
                    JIRA_SERVER,
                    JIRA_DATA_CENTER,
                ]:
                    access_control.add(prefix_account_id(account_id=actor.get("name")))
                    access_control.add(
                        prefix_account_name(account_name=actor.get("displayName"))
                    )
        return list(access_control)

    async def _cache_project_access_control(self, project):
        project_key = project.get("key")
        if project_key in self.project_permission_cache.keys():
            project_access_controls = self.project_permission_cache.get(project_key)
        else:
            project_access_controls = await self._project_access_control(
                project=project
            )
            self.project_permission_cache[project_key] = project_access_controls
        return project_access_controls

    async def _issue_access_control(self, issue_key, project):
        if not self._dls_enabled():
            return []

        self._logger.debug(
            f"Fetching users with read access to issue '{issue_key}' in project '{project['key']}'"
        )
        access_control = set()
        if self.jira_client.data_source_type != JIRA_CLOUD:
            async for actors in self.jira_client.user_information_list(
                key=f"issueKey={issue_key}"
            ):
                for actor in actors:
                    access_control.add(prefix_account_id(account_id=actor.get("name")))
                    access_control.add(
                        prefix_account_name(account_name=actor.get("displayName"))
                    )
            return list(access_control)

        async for response in self.jira_client.issue_security_level(
            issue_key=issue_key
        ):
            if security := response.get("fields", {}).get("security"):
                level_id = security.get("id")
                async for members in self.jira_client.issue_security_level_members(
                    level_id=level_id
                ):
                    for actor in members["values"]:
                        actor_type = actor.get("holder", {}).get("type")
                        if actor_type == "user":
                            user = actor.get("holder", {}).get("user", {})
                            if self.atlassian_access_control.is_active_atlassian_user(
                                user_info=user
                            ):
                                access_control.add(
                                    prefix_account_id(account_id=user.get("accountId"))
                                )
                                access_control.add(
                                    prefix_account_name(
                                        account_name=user.get("displayName")
                                    )
                                )
                        elif actor_type == "group":
                            group_id = (
                                actor.get("holder", {}).get("group", {}).get("groupId")
                            )
                            access_control.add(prefix_group_id(group_id=group_id))
                        elif actor_type == "projectRole":
                            if (
                                role_id := actor.get("holder", {})
                                .get("projectRole", {})
                                .get("id")
                            ):
                                # Project Role - `atlassian-addons-project-access` with id 10003 is not needed for DLS
                                is_addons_projects_access = role_id == 10003
                                if not is_addons_projects_access:
                                    access_control = await anext(
                                        self.jira_client.project_role_members(
                                            project=project,
                                            role_id=role_id,
                                            access_control=access_control,
                                        )
                                    )
            else:
                self._logger.debug(
                    f"Issue security level is not set for an issue: {issue_key}. Hence, Assigning project permissions"
                )
                project_access_controls = await self._cache_project_access_control(
                    project=project
                )

                return project_access_controls
        return list(access_control)

    async def get_access_control(self):
        """Get access control documents for active Atlassian users.

        This method fetches access control documents for active Atlassian users when document level security (DLS)
        is enabled. It starts by checking if DLS is enabled, and if not, it logs a warning message and skips further processing.
        If DLS is enabled, the method fetches all users from the Jira API, filters out active Atlassian users,
        and fetches additional information for each active user using the _fetch_user method. After gathering the user information,
        it generates an access control document for each user using the user_access_control_doc method and yields the results.

        Yields:
            dict: An access control document for each active Atlassian user.
        """
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching all users for Access Control sync")

        users_endpoint = (
            URLS[USERS]
            if self.jira_client.data_source_type == JIRA_CLOUD
            else URLS[USERS_FOR_DATA_CENTER]
        )
        url = parse.urljoin(self.jira_client.host_url, users_endpoint)
        async for users in self.atlassian_access_control.fetch_all_users(url=url):
            active_atlassian_users = filter(
                self.atlassian_access_control.is_active_atlassian_user, users
            )
            tasks = [
                anext(
                    self.atlassian_access_control.fetch_user(
                        url=f"{user_info.get('self')}&{USER_QUERY}"
                    )
                )
                for user_info in active_atlassian_users
            ]
            user_results = await asyncio.gather(*tasks)

            for user in user_results:
                yield await self.atlassian_access_control.user_access_control_doc(
                    user=user
                )

    def advanced_rules_validators(self):
        return [AtlassianAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by jira

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        """Closes unclosed client session"""
        await self.jira_client.close_session()

    async def get_content(self, issue_key, attachment, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            issue_key (str): Issue key to generate `_id` for attachment document
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        file_size = int(attachment["size"])
        if not (doit and file_size > 0):
            return

        filename = attachment["filename"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        self._logger.debug(f"Downloading content for file: {filename}")
        download_url = (
            ATTACHMENT_CLOUD
            if self.jira_client.data_source_type == JIRA_CLOUD
            else ATTACHMENT_SERVER
        )

        document = {
            "_id": f"{issue_key}-{attachment['id']}",
            "_timestamp": attachment["created"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.jira_client.api_call,
                    url_name=download_url,
                    attachment_id=attachment["id"],
                    attachment_name=attachment["filename"],
                ),
            ),
        )

    async def ping(self):
        """Verify the connection with Jira"""
        try:
            await self.jira_client.ping()
            self._logger.debug("Successfully connected to the Jira")
        except Exception:
            self._logger.exception("Error while connecting to the Jira")
            raise

    async def _put_projects(self, project, timestamp):
        """Store project documents to queue

        Args:
            project (dict): Project document to store in queue
            timestamp (str): Timestamp to manage project document
        """
        document = {
            "_id": f"project-{project['id']}",
            "_timestamp": timestamp,
            "Key": project.get("key"),
            "Type": "Project",
            "Project": project,
        }
        project_access_control = await self._cache_project_access_control(
            project=project
        )
        document_with_access_control = self._decorate_with_access_control(
            document=document, access_control=project_access_control
        )
        await self.queue.put((document_with_access_control, None))  # pyright: ignore

    async def _get_projects(self):
        """Get projects with the help of REST APIs

        Yields:
            project: Project document to get indexed
        """
        try:
            timezone = await self.jira_client.get_timezone()

            timestamp = iso_utc(
                when=datetime.now(pytz.timezone(timezone))  # pyright: ignore
            )
            async for project in self.jira_client.get_projects():
                await self._put_projects(project=project, timestamp=timestamp)
            await self.queue.put("FINISHED")  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {PROJECT}. Error: {exception}"
            )

    async def _put_issue(self, issue):
        """Put a specific issue as per the given issue_key in a queue

        Args:
            issue (dict): Issue representation containing the key to enqueue
        """
        async for issue_metadata in self.jira_client.get_issues_for_issue_key(
            key=issue.get("key")
        ):
            response_custom_fields = {}
            response_fields = copy(issue_metadata.get("fields"))
            for k, v in response_fields.items():
                if self.custom_fields.get(k):
                    response_custom_fields[self.custom_fields[k]] = v

            for k in self.custom_fields.keys():
                if k in response_fields:
                    del response_fields[k]

            document = {
                "_id": f"{response_fields.get('project', {}).get('name')}-{issue_metadata.get('key')}",
                "_timestamp": response_fields.get("updated"),
                "Key": issue_metadata.get("key"),
                "Type": response_fields.get("issuetype", {}).get("name"),
                "Issue": response_fields,
                "Custom_Fields": response_custom_fields,
            }

            if restrictions := [
                restriction.get("restrictionValue")
                for restriction in response_fields.get("issuerestriction", {})
                .get("issuerestrictions", {})
                .get("projectrole", [])
            ]:
                issue_access_control = []
                for role_id in restrictions:
                    access_control = await anext(
                        self.jira_client.project_role_members(
                            project=response_fields.get("project"),
                            role_id=role_id,
                            access_control=set(),
                        )
                    )
                    issue_access_control.extend(list(access_control))
            else:
                issue_access_control = await self._issue_access_control(
                    issue_key=issue_metadata.get("key"),
                    project=response_fields.get("project"),
                )
            document_with_access_control = self._decorate_with_access_control(
                document=document, access_control=issue_access_control
            )
            await self.queue.put((document_with_access_control, None))  # pyright: ignore
            attachments = issue_metadata.get("fields", {}).get("attachment")
            if len(attachments) > 0:
                await self._put_attachment(
                    attachments=attachments,
                    issue_key=issue_metadata.get("key"),
                    access_control=issue_access_control,
                )
        await self.queue.put("FINISHED")  # pyright: ignore

    async def _get_issues(self, custom_query=""):
        """Get issues with the help of REST APIs

        Yields:
            Dictionary: Jira issue to get indexed
            issue (dict): Issue response to fetch the attachments
        """
        wildcard_query = "key%20IS%20NOT%20EMPTY"
        comma_separated_projects = '"' + '","'.join(self.jira_client.projects) + '"'
        projects_query = f"project in ({comma_separated_projects})"

        jql = custom_query or (
            wildcard_query
            if self.jira_client.projects == [WILDCARD]
            else projects_query
        )

        async for issue in self.jira_client.get_issues_for_jql(jql=jql):
            await self.fetchers.put(partial(self._put_issue, issue))
            self.tasks += 1
        await self.queue.put("FINISHED")  # pyright: ignore

    async def _put_attachment(self, attachments, issue_key, access_control):
        """Put attachments of a specific issue in a queue

        Args:
            attachments (list): List of attachments for an issue
            issue_key (str): Issue key for generating `_id` field
        """
        self._logger.debug(f"Fetching attachments for issue: {issue_key}")
        for attachment in attachments:
            document = {
                "_id": f"{issue_key}-{attachment['id']}",
                "title": attachment["filename"],
                "Type": "Attachment",
                "issue": issue_key,
                "_timestamp": attachment["created"],
                "size": attachment["size"],
            }
            document_with_access_control = self._decorate_with_access_control(
                document=document, access_control=access_control
            )
            await self.queue.put(
                (  # pyright: ignore
                    document_with_access_control,
                    partial(
                        self.get_content,
                        issue_key=issue_key,
                        attachment=copy(attachment),
                    ),
                )
            )

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Jira.
        """
        while self.tasks > 0:
            _, item = await self.queue.get()
            if item == FINISHED:
                self.tasks -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch jira objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        self.custom_fields = await anext(self.jira_client.get_jira_fields())

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            self._logger.info(
                f"Fetching jira content using advanced sync rules: {advanced_rules}"
            )

            for rule in advanced_rules:
                query = rule.get("query", "")
                self._logger.debug(f"Fetching issues using query: {query}")
                await self.fetchers.put(partial(self._get_issues, query))
                self.tasks += 1

        else:
            self._logger.info("Fetching jira content without advanced sync rules")
            await self.jira_client.verify_projects()

            await self.fetchers.put(self._get_projects)
            await self.fetchers.put(self._get_issues)
            self.tasks += 2

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
