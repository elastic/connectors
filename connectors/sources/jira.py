#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Jira source module responsible to fetch documents from Jira on-prem or cloud server.
"""
import asyncio
import os
from copy import copy
from datetime import datetime
from functools import partial
from urllib import parse

import aiofiles
import aiohttp
import pytz
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.sources.atlassian import AtlassianAdvancedRulesValidator
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    convert_to_b64,
    iso_utc,
    ssl_context,
)

RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760

FETCH_SIZE = 100
CHUNK_SIZE = 1024
QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes
MAX_CONCURRENCY = 5
MAX_CONCURRENT_DOWNLOADS = 100  # Max concurrent download supported by jira

PING = "ping"
PROJECT = "project"
PROJECT_BY_KEY = "project_by_key"
ISSUES = "all_issues"
ISSUE_DATA = "issue_data"
ATTACHMENT_CLOUD = "attachment_cloud"
ATTACHMENT_SERVER = "attachment_server"
USERS = "users"
PROJECT_PERMISSIONS_BY_KEY = "project_permissions_by_key"
ISSUE_SECURITY_LEVEL = "issue_security_level"
SECURITY_LEVEL_MEMBERS = "issue_security_members"
PROJECT_ROLE_MEMBERS_BY_ROLE_ID = "project_role_members_by_role_id"
URLS = {
    PING: "/rest/api/2/myself",
    PROJECT: "/rest/api/2/project?expand=description,lead,url",
    PROJECT_BY_KEY: "/rest/api/2/project/{key}",
    ISSUES: "/rest/api/2/search?jql={jql}&maxResults={max_results}&startAt={start_at}",
    ISSUE_DATA: "/rest/api/2/issue/{id}",
    ATTACHMENT_CLOUD: "/rest/api/2/attachment/content/{attachment_id}",
    ATTACHMENT_SERVER: "/secure/attachment/{attachment_id}/{attachment_name}",
    USERS: "/rest/api/3/users/search",
    PROJECT_PERMISSIONS_BY_KEY: "/rest/api/2/user/permission/search?projectKey={project_key}&permissions=BROWSE_PROJECTS",
    PROJECT_ROLE_MEMBERS_BY_ROLE_ID: "/rest/api/3/project/{project_key}/role/{role_id}",
    ISSUE_SECURITY_LEVEL: "/rest/api/2/issue/{issue_key}?fields=security",
    SECURITY_LEVEL_MEMBERS: "/rest/api/3/issuesecurityschemes/level/member?maxResults={max_results}&startAt={start_at}&levelId={level_id}&expand=user,group,projectRole",
}

JIRA_CLOUD = "jira_cloud"
JIRA_SERVER = "jira_server"

ATLASSIAN = "atlassian"
USER_QUERY = "expand=groups,applicationRoles"


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_account_id(user_id):
    return prefix_identity("account_id", user_id)


def _prefix_group_id(group_id):
    return prefix_identity("group", group_id)


def _prefix_role_key(role_key):
    return prefix_identity("application_role", role_key)


class JiraClient:
    """Jira client to handle API calls made to Jira"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.is_cloud = self.configuration["data_source"] == JIRA_CLOUD
        self.host_url = self.configuration["jira_url"]
        self.projects = self.configuration["projects"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]

        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False
        self.session = None

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Generate and return base client session with configuration fields

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        if self.session:
            return self.session
        if self.is_cloud:
            login, password = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        else:
            login, password = (
                self.configuration["username"],
                self.configuration["password"],
            )

        basic_auth = aiohttp.BasicAuth(login=login, password=password)
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore
        self.session = aiohttp.ClientSession(
            auth=basic_auth,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()
        self.session = None

    async def api_call(self, url_name=None, **url_kwargs):
        """Make a GET call for Atlassian API using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            url_kwargs (dict): Url kwargs to format the query.

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Return api response.
        """
        retry = 0
        url = url_kwargs.get("url") or parse.urljoin(
            self.host_url, URLS[url_name].format(**url_kwargs)  # pyright: ignore
        )
        while True:
            try:
                async with self._get_session().get(  # pyright: ignore
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    yield response
                    break
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close_session()
                retry += 1
                if retry > self.retry_count:
                    raise exception
                self._logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry)

    async def paginated_api_call(self, url_name, jql=None, **kwargs):
        """Make a paginated API call for Jira objects using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            jql (str, None): Jira Query Language to filter the issues.

        Yields:
            response: Return api response.
        """
        start_at = 0

        while True:
            try:
                url = None
                if kwargs.get("level_id"):
                    url = parse.urljoin(
                        self.host_url,
                        URLS[url_name].format(
                            max_results=FETCH_SIZE,
                            start_at=start_at,
                            level_id=kwargs.get("level_id"),
                        ),  # pyright: ignore
                    )
                async for response in self.api_call(
                    url_name=url_name,
                    start_at=start_at,
                    max_results=FETCH_SIZE,
                    jql=jql,
                    url=url,
                ):
                    response_json = await response.json()
                    total = response_json["total"]
                    yield response_json
                    if start_at + FETCH_SIZE > total or total <= FETCH_SIZE:
                        return
                    start_at += FETCH_SIZE
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type: {url_name}, query params: jql={jql}, startAt={start_at}, maxResults={FETCH_SIZE}. Error: {exception}."
                )
                break


class JiraDataSource(BaseDataSource):
    """Jira"""

    name = "Jira"
    service_type = "jira"
    advanced_rules_enabled = True
    dls_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Jira

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.jira_client = JiraClient(configuration=configuration)

        self.tasks = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)

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
                "value": "admin",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": JIRA_SERVER}],
                "label": "Jira Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
                "value": "changeme",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud service account id",
                "order": 4,
                "type": "str",
                "value": "me@example.com",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Jira Cloud API token",
                "order": 5,
                "sensitive": True,
                "type": "str",
                "value": "abc#123",
            },
            "jira_url": {
                "label": "Jira host url",
                "order": 6,
                "type": "str",
                "value": "http://127.0.0.1:8080",
            },
            "projects": {
                "display": "textarea",
                "label": "Jira project keys",
                "order": 7,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": "*",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 8,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 9,
                "type": "str",
                "value": "",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Retries for failed requests",
                "order": 10,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "value": 3,
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 11,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
                "validations": [
                    {"type": "less_than", "constraint": MAX_CONCURRENT_DOWNLOADS + 1}
                ],
                "value": MAX_CONCURRENT_DOWNLOADS,
            },
            "use_document_level_security": {
                "display": "toggle",
                "depends_on": [{"field": "data_source", "value": JIRA_CLOUD}],
                "label": "Enable document level security",
                "order": 12,
                "tooltip": "Document level security ensures identities and permissions set in Jira are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
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

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    def _is_active_atlassian_user(self, user_info):
        user_url = user_info.get("self")
        user_name = user_info.get("displayName", "user")
        if not user_url:
            self._logger.debug(
                f"Skipping user: {user_name} due to the absence of a personal URL."
            )
            return False

        if not user_info.get("active"):
            self._logger.debug(
                f"Skipping user: {user_name} as it is inactive or deleted."
            )
            return False

        if user_info.get("accountType") != ATLASSIAN:
            self._logger.debug(
                f"Skipping user: {user_name} because the account type is {user_info.get('accountType')}. Only 'atlassian' account type is supported."
            )
            return False

        return True

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )

        return document

    async def _user_information_list(self, project):
        async for response in self.jira_client.api_call(
            url_name=PROJECT_PERMISSIONS_BY_KEY, project_key=project["key"]
        ):
            yield await response.json()

    async def _project_access_control(self, project):
        if not self._dls_enabled():
            return []

        access_control = set()
        async for actors in self._user_information_list(project=project):
            for actor in actors:
                if actor["accountType"] == ATLASSIAN:
                    access_control.add(
                        _prefix_account_id(user_id=actor.get("accountId"))
                    )
                    access_control.add(_prefix_username(user=actor.get("displayName")))
        return list(access_control)

    async def _issue_security_level(self, issue_key):
        async for response in self.jira_client.api_call(
            url_name=ISSUE_SECURITY_LEVEL, issue_key=issue_key
        ):
            yield await response.json()

    async def _issue_security_level_members(self, level_id):
        async for response in self.jira_client.paginated_api_call(
            url_name=SECURITY_LEVEL_MEMBERS, level_id=level_id
        ):
            yield response

    async def _project_role_members(self, project, role_id, access_control):
        async for actor_response in self.jira_client.api_call(
            url_name=PROJECT_ROLE_MEMBERS_BY_ROLE_ID,
            project_key=project["key"],
            role_id=role_id,
        ):
            actors = await actor_response.json()
            for actor in actors.get("actors", []):
                if actor.get("actorUser"):
                    access_control.add(
                        _prefix_account_id(
                            user_id=actor.get("actorUser").get("accountId")
                        )
                    )
                    access_control.add(_prefix_username(user=actor.get("displayName")))
                elif actor.get("actorGroup"):
                    access_control.add(
                        _prefix_group_id(
                            group_id=actor.get("actorGroup").get("groupId")
                        )
                    )
            yield access_control

    async def _issue_access_control(self, issue_key, project):
        if not self._dls_enabled():
            return []

        access_control = set()
        async for response in self._issue_security_level(issue_key=issue_key):
            if security := response.get("fields", {}).get("security"):
                level_id = security.get("id")
                async for members in self._issue_security_level_members(
                    level_id=level_id
                ):
                    for actor in members["values"]:
                        actor_type = actor.get("holder", {}).get("type")
                        if actor_type == "user":
                            user = actor.get("holder", {}).get("user", {})
                            if self._is_active_atlassian_user(user_info=user):
                                access_control.add(
                                    _prefix_account_id(user_id=user.get("accountId"))
                                )
                                access_control.add(
                                    _prefix_username(user=user.get("displayName"))
                                )
                        elif actor_type == "group":
                            group_id = (
                                actor.get("holder", {}).get("group", {}).get("groupId")
                            )
                            access_control.add(_prefix_group_id(group_id=group_id))
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
                                        self._project_role_members(
                                            project=project,
                                            role_id=role_id,
                                            access_control=access_control,
                                        )
                                    )
            else:
                self._logger.debug(
                    f"Issue security level is not set for an issue: {issue_key}. Hence, Assigning project permissions"
                )
                project_access_controls = await self._project_access_control(
                    project=project
                )
                return project_access_controls
        return list(access_control)

    async def _user_access_control_doc(self, user):
        """Constructs a user access control document, which will be synced to the corresponding access control index.
        The `_id` of the user access control document will either be the username (can also be the email sometimes) or the email itself.
        Note: the `_id` field won't be prefixed with the corresponding identity prefix ("user" or "email").
        The document contains all groups of a user and his email and/or username under `query.template.params.access_control`.

        Returns:
            dict: dictionary representing an user access control document
            {
                "_id": "some.user@spo.com",
                "identity": {
                    "username": "username:some.user",
                    "account_id": "account_id:some user id"
                },
                "created_at": "2023-06-30 12:00:00",
                "query": {
                    "template": {
                        "params": {
                            "access_control": [
                                "username:some.user",
                                "group:1234-abcd-id"
                            ]
                        }
                    }
                }
            }
        """
        account_id = user.get("accountId")
        account_name = user.get("displayName")

        _prefixed_account_id = _prefix_account_id(user_id=account_id)
        _prefixed_user_name = _prefix_username(user=account_name)

        _prefixed_group_ids = {
            _prefix_group_id(group_id=group.get("groupId", ""))
            for group in user.get("groups", {}).get("items", [])
        }
        _prefixed_role_keys = {
            _prefix_role_key(role_key=role.get("key", ""))
            for role in user.get("applicationRoles", {}).get("items", [])
        }

        user_document = {
            "_id": account_id,
            "identity": {
                "account_id": _prefixed_account_id,
                "username": _prefixed_user_name,
            },
            "created_at": iso_utc(),
        }

        access_control = (
            [_prefixed_account_id]
            + list(_prefixed_group_ids)
            + list(_prefixed_role_keys)
        )

        return user_document | self.access_control_query(access_control=access_control)

    async def _fetch_all_users(self):
        async for users in self.jira_client.api_call(url_name=USERS):
            yield await users.json()

    async def _fetch_user(self, user_url):
        async for user in self.jira_client.api_call(url=f"{user_url}&{USER_QUERY}"):
            yield await user.json()

    async def get_access_control(self):
        """Get access control documents for active Atlassian users.

        This method fetches access control documents for active Atlassian users when document level security (DLS)
        is enabled. It starts by checking if DLS is enabled, and if not, it logs a warning message and skips further processing.
        If DLS is enabled, the method fetches all users from the Jira API, filters out active Atlassian users,
        and fetches additional information for each active user using the _fetch_user method. After gathering the user information,
        it generates an access control document for each user using the _user_access_control_doc method and yields the results.

        Yields:
            dict: An access control document for each active Atlassian user. The access control document has the following structure:
            {
                "_id": <account_id>,
                "identity": {
                    "account_id": <_prefixed_account_id>,
                    "display_name": <_prefixed_account_name>
                },
                "created_at": <iso_utc_timestamp>,
                ACCESS_CONTROL: [<_prefixed_account_id>, <_prefixed_group_ids>, <_prefixed_role_keys>]
            }
        """
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        users = await anext(self._fetch_all_users())
        active_atlassian_users = filter(self._is_active_atlassian_user, users)

        tasks = [
            anext(self._fetch_user(user_url=user_info.get("self")))
            for user_info in active_atlassian_users
        ]
        user_results = await asyncio.gather(*tasks)

        for user in user_results:
            yield await self._user_access_control_doc(user=user)

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
        attachment_size = int(attachment["size"])
        if not (doit and attachment_size > 0):
            return

        attachment_name = attachment["filename"]
        if (
            os.path.splitext(attachment_name)[-1]
        ).lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"{attachment_name} is not supported by TIKA, skipping"
            )
            return

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": f"{issue_key}-{attachment['id']}",
            "_timestamp": attachment["created"],
        }
        temp_filename = ""
        attachment_url = (
            ATTACHMENT_CLOUD if self.jira_client.is_cloud else ATTACHMENT_SERVER
        )
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self.jira_client.api_call(
                url_name=attachment_url,
                attachment_id=attachment["id"],
                attachment_name=attachment["filename"],
            ):
                async for data in response.content.iter_chunked(CHUNK_SIZE):
                    await async_buffer.write(data)
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

    async def ping(self):
        """Verify the connection with Jira"""
        try:
            await anext(self.jira_client.api_call(url_name=PING))
            self._logger.debug("Successfully connected to the Jira")
        except Exception:
            self._logger.exception("Error while connecting to the Jira")
            raise

    async def _verify_projects(self):
        """Checks if user configured projects are available in jira

        Raises:
            Exception: Configured unavailable projects: <unavailable_project_keys>
        """
        if self.jira_client.projects == ["*"]:
            return
        project_keys = []
        try:
            async for response in self.jira_client.api_call(url_name=PROJECT):
                response = await response.json()
                project_keys = [project["key"] for project in response]
            if unavailable_projects := set(self.jira_client.projects) - set(
                project_keys
            ):
                raise Exception(
                    f"Configured unavailable projects: {', '.join(unavailable_projects)}"
                )
        except Exception as exception:
            raise Exception(
                f"Unable to verify projects: {self.jira_client.projects}. Error: {exception}"
            ) from exception

    async def _get_timezone(self):
        """Returns the timezone of the Jira deployment"""
        async for response in self.jira_client.api_call(url_name=PING):
            timezone = await response.json()
            return timezone["timeZone"]

    async def _put_projects(self, project, timestamp):
        """Store project documents to queue

        Args:
            project (dict): Project document to store in queue
            timestamp (str): Timestamp to manage project document
        """
        document = {
            "_id": f"project-{project['id']}",
            "_timestamp": timestamp,
            "Type": "Project",
            "Project": project,
        }
        project_access_control = await self._project_access_control(project=project)
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
            timezone = await self._get_timezone()

            timestamp = iso_utc(
                when=datetime.now(pytz.timezone(timezone))  # pyright: ignore
            )
            if self.jira_client.projects == ["*"]:
                async for response in self.jira_client.api_call(url_name=PROJECT):
                    response = await response.json()
                    for project in response:
                        await self._put_projects(project=project, timestamp=timestamp)
            else:
                for project_key in self.jira_client.projects:
                    async for response in self.jira_client.api_call(
                        url_name=PROJECT_BY_KEY, key=project_key
                    ):
                        project = await response.json()
                        await self._put_projects(project=project, timestamp=timestamp)
            await self.queue.put("FINISHED")  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {PROJECT}. Error: {exception}"
            )

    async def _put_issue(self, issue):
        """Put specific issue as per the given issue_key in a queue

        Args:
            issue (str): Issue key to fetch an issue
        """
        try:
            async for response in self.jira_client.api_call(
                url_name=ISSUE_DATA, id=issue["key"]
            ):
                issue = await response.json()
                response_fields = issue.get("fields")
                document = {
                    "_id": f"{response_fields['project']['name']}-{issue['key']}",
                    "_timestamp": response_fields["updated"],
                    "Type": response_fields["issuetype"]["name"],
                    "Issue": response_fields,
                }
                issue_access_control = await self._issue_access_control(
                    issue_key=issue["key"], project=response_fields["project"]
                )
                document_with_access_control = self._decorate_with_access_control(
                    document=document, access_control=issue_access_control
                )
                await self.queue.put(
                    (document_with_access_control, None)
                )  # pyright: ignore
                attachments = issue["fields"]["attachment"]
                if len(attachments) > 0:
                    await self._put_attachment(
                        attachments=attachments,
                        issue_key=issue["key"],
                        access_control=issue_access_control,
                    )
            await self.queue.put("FINISHED")  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Skipping data for type: {ISSUE_DATA}. Error: {exception}"
            )

    async def _get_issues(self, custom_query=""):
        """Get issues with the help of REST APIs

        Yields:
            Dictionary: Jira issue to get indexed
            issue (dict): Issue response to fetch the attachments
        """
        wildcard_query = ""
        projects_query = f"project in ({','.join(self.jira_client.projects)})"

        jql = custom_query or (
            wildcard_query if self.jira_client.projects == ["*"] else projects_query
        )

        async for response in self.jira_client.paginated_api_call(
            url_name=ISSUES, jql=jql
        ):
            for issue in response.get("issues", []):
                await self.fetchers.put(partial(self._put_issue, issue))
                self.tasks += 1
        await self.queue.put("FINISHED")  # pyright: ignore

    async def _put_attachment(self, attachments, issue_key, access_control):
        """Put attachments of a specific issue in a queue

        Args:
            attachments (list): List of attachments for an issue
            issue_key (str): Issue key for generating `_id` field
        """
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
            if item == "FINISHED":
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
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            for rule in advanced_rules:
                await self.fetchers.put(
                    partial(self._get_issues, rule.get("query", ""))
                )
                self.tasks += 1

        else:
            await self._verify_projects()

            await self.fetchers.put(self._get_projects)
            await self.fetchers.put(self._get_issues)
            self.tasks += 2

        async for item in self._consumer():
            yield item

        await self.fetchers.join()
