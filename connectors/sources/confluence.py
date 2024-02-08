#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Confluence source module responsible to fetch documents from Confluence Cloud/Server.
"""
import asyncio
import os
from copy import copy
from functools import partial

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.access_control import ACCESS_CONTROL
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.atlassian import (
    AtlassianAccessControl,
    AtlassianAdvancedRulesValidator,
    prefix_account_id,
    prefix_group_id,
)
from connectors.utils import (
    CancellableSleeps,
    ConcurrentTasks,
    MemQueue,
    iso_utc,
    ssl_context,
)

RETRY_INTERVAL = 2
SPACE = "space"
BLOGPOST = "blogpost"
PAGE = "page"
ATTACHMENT = "attachment"
CONTENT = "content"
DOWNLOAD = "download"
SEARCH = "search"
USER = "user"
SPACE_QUERY = "limit=100&expand=permissions"
ATTACHMENT_QUERY = "limit=100&expand=version"
CONTENT_QUERY = "limit=50&expand=children.attachment,history.lastUpdated,body.storage,space,space.permissions,restrictions.read.restrictions.user,restrictions.read.restrictions.group"
SEARCH_QUERY = "limit=100&expand=content.extensions,content.container,content.space,space.description"
USER_QUERY = "expand=groups,applicationRoles"

URLS = {
    SPACE: "rest/api/space?{api_query}",
    CONTENT: "rest/api/content/search?{api_query}",
    ATTACHMENT: "rest/api/content/{id}/child/attachment?{api_query}",
    SEARCH: "rest/api/search?cql={query}",
    USER: "rest/api/3/users/search",
}
PING_URL = "rest/api/space?limit=1"
MAX_CONCURRENT_DOWNLOADS = 50  # Max concurrent download supported by confluence
MAX_CONCURRENCY = 50
QUEUE_SIZE = 1024
QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
END_SIGNAL = "FINISHED_TASK"

CONFLUENCE_CLOUD = "confluence_cloud"
CONFLUENCE_SERVER = "confluence_server"
CONFLUENCE_DATA_CENTER = "confluence_data_center"
WILDCARD = "*"


class ConfluenceClient:
    """Confluence client to handle API calls made to Confluence"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.data_source_type = self.configuration["data_source"]
        self.host_url = self.configuration["confluence_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]
        self.retry_count = self.configuration["retry_count"]
        if self.data_source_type == CONFLUENCE_CLOUD:
            self.host_url = os.path.join(self.host_url, "wiki")

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
        if self.data_source_type == CONFLUENCE_CLOUD:
            auth = (
                self.configuration["account_email"],
                self.configuration["api_token"],
            )
        elif self.data_source_type == CONFLUENCE_SERVER:
            auth = (
                self.configuration["username"],
                self.configuration["password"],
            )
        else:
            auth = (
                self.configuration["data_center_username"],
                self.configuration["data_center_password"],
            )

        basic_auth = aiohttp.BasicAuth(login=auth[0], password=auth[1])
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

    async def api_call(self, url):
        """Make a GET call for Atlassian API using the passed url with retry for the failed API calls.

        Args:
            url: Request URL to hit the get call

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Client response
        """
        retry_counter = 0
        while True:
            try:
                async with self._get_session().get(
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
                    await self.session.close()  # pyright: ignore
                retry_counter += 1
                if retry_counter > self.retry_count:
                    raise exception
                self._logger.warning(
                    f"Retry count: {retry_counter} out of {self.retry_count}. Exception: {exception}"
                )
                await self._sleeps.sleep(RETRY_INTERVAL**retry_counter)

    async def paginated_api_call(self, url_name, **url_kwargs):
        """Make a paginated API call for Confluence objects using the passed url_name.
        Args:
            url_name (str): URL Name to identify the API endpoint to hit
        Yields:
            response: JSON response.
        """
        url = os.path.join(self.host_url, URLS[url_name].format(**url_kwargs))
        while True:
            try:
                async for response in self.api_call(
                    url=url,
                ):
                    json_response = await response.json()
                    links = json_response.get("_links")
                    yield json_response
                    if links.get("next") is None:
                        return
                    url = os.path.join(
                        self.host_url,
                        links.get("next")[1:],
                    )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type {url_name} from {url}. Exception: {exception}."
                )
                break


class ConfluenceDataSource(BaseDataSource):
    """Confluence"""

    name = "Confluence"
    service_type = "confluence"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to Confluence

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.spaces = self.configuration["spaces"]
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.confluence_client = ConfluenceClient(configuration=configuration)
        self.atlassian_access_control = AtlassianAccessControl(
            self, self.confluence_client
        )

        self.queue = MemQueue(maxsize=QUEUE_SIZE, maxmemsize=QUEUE_MEM_SIZE)
        self.fetchers = ConcurrentTasks(max_concurrency=MAX_CONCURRENCY)
        self.fetcher_count = 0

    def _set_internal_logger(self):
        self.confluence_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Confluence

        Returns:
            dictionary: Default configuration.
        """
        return {
            "data_source": {
                "display": "dropdown",
                "label": "Confluence data source",
                "options": [
                    {"label": "Confluence Cloud", "value": CONFLUENCE_CLOUD},
                    {"label": "Confluence Server", "value": CONFLUENCE_SERVER},
                    {
                        "label": "Confluence Data Center",
                        "value": CONFLUENCE_DATA_CENTER,
                    },
                ],
                "order": 1,
                "type": "str",
                "value": CONFLUENCE_SERVER,
            },
            "username": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_SERVER}],
                "label": "Confluence Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "data_center_username": {
                "depends_on": [
                    {"field": "data_source", "value": CONFLUENCE_DATA_CENTER}
                ],
                "label": "Confluence Data Center username",
                "order": 4,
                "type": "str",
            },
            "data_center_password": {
                "depends_on": [
                    {"field": "data_source", "value": CONFLUENCE_DATA_CENTER}
                ],
                "label": "Confluence Data Center password",
                "sensitive": True,
                "order": 5,
                "type": "str",
            },
            "account_email": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud account email",
                "order": 6,
                "type": "str",
            },
            "api_token": {
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "label": "Confluence Cloud API token",
                "sensitive": True,
                "order": 7,
                "type": "str",
            },
            "confluence_url": {
                "label": "Confluence URL",
                "order": 8,
                "type": "str",
            },
            "spaces": {
                "display": "textarea",
                "label": "Confluence space keys",
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
                "label": "Retries per request",
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
                "depends_on": [{"field": "data_source", "value": CONFLUENCE_CLOUD}],
                "display": "toggle",
                "label": "Enable document level security",
                "order": 14,
                "tooltip": "Document level security ensures identities and permissions set in confluence are maintained in Elasticsearch. This enables you to restrict and personalize read-access users have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
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

    async def get_access_control(self):
        """Get access control documents for active Atlassian users.

        This method fetches access control documents for active Atlassian users when document level security (DLS)
        is enabled. It starts by checking if DLS is enabled, and if not, it logs a warning message and skips further processing.
        If DLS is enabled, the method fetches all users from the Confluence API, filters out active Atlassian users,
        and fetches additional information for each active user using the fetch_user method. After gathering the user information,
        it generates an access control document for each user using the user_access_control_doc method and yields the results.

        Yields:
            dict: An access control document for each active Atlassian user.
        """
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching all users")
        url = os.path.join(self.configuration["confluence_url"], URLS[USER])
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

    def _get_access_control_from_permission(self, permissions, target_type):
        if not self._dls_enabled():
            return []

        access_control = set()
        for permission in permissions:
            permission_operation = permission.get("operation", {})
            if permission_operation.get("targetType") != target_type and (
                permission_operation.get("targetType") != SPACE
                and permission_operation.get("operation") != "read"
            ):
                continue

            access_control = access_control.union(
                self._extract_identities(response=permission.get("subjects", {}))
            )

        return access_control

    def _extract_identities(self, response):
        if not self._dls_enabled():
            return set()

        identities = set()
        user_results = response.get("user", {}).get("results", [])
        group_results = response.get("group", {}).get("results", [])

        for item in user_results + group_results:
            item_type = item.get("type")
            if item_type == "known" and item.get("accountType") == "atlassian":
                identities.add(prefix_account_id(account_id=item.get("accountId", "")))
            elif item_type == "group":
                identities.add(prefix_group_id(group_id=item.get("id", "")))

        return identities

    async def close(self):
        """Closes unclosed client session"""
        await self.confluence_client.close_session()

    def advanced_rules_validators(self):
        return [AtlassianAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by Confluence

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.
        Also validate, if user configured spaces are available in Confluence.

        Raises:
            Exception: Configured keys can't be empty
        """
        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        if self.spaces == [WILDCARD]:
            return
        space_keys = []
        async for response in self.confluence_client.paginated_api_call(
            url_name=SPACE, api_query=SPACE_QUERY
        ):
            spaces = response.get("results", [])
            space_keys.extend([space["key"] for space in spaces])
        if unavailable_spaces := set(self.spaces) - set(space_keys):
            msg = f"Spaces '{', '.join(unavailable_spaces)}' are not available. Available spaces are: '{', '.join(space_keys)}'"
            raise ConfigurableFieldValueError(msg)

    async def ping(self):
        """Verify the connection with Confluence"""
        try:
            await anext(
                self.confluence_client.api_call(
                    url=os.path.join(self.confluence_client.host_url, PING_URL),
                )
            )
            self._logger.info("Successfully connected to Confluence")
        except Exception:
            self._logger.exception("Error while connecting to Confluence")
            raise

    async def fetch_spaces(self):
        """Get spaces with the help of REST APIs

        Yields:
            Dictionary: Space document to get indexed
            List: List of permissions attached to space
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=SPACE,
            api_query=SPACE_QUERY,
        ):
            for space in response.get("results", []):
                space_url = os.path.join(
                    self.confluence_client.host_url, space["_links"]["webui"][1:]
                )
                if (self.spaces == [WILDCARD]) or (space["key"] in self.spaces):
                    yield {
                        "_id": space["id"],
                        "type": "Space",
                        "title": space["name"],
                        "_timestamp": iso_utc(),
                        "url": space_url,
                    }, space.get("permissions", [])

    async def fetch_documents(self, api_query):
        """Get pages and blog posts with the help of REST APIs

        Args:
            api_query (str): Query parameter for the API call

        Yields:
            Dictionary: Page or blog post to be indexed
            Integer: Number of attachments in a page/blogpost
            List: List of permissions attached to document
            Dictionary: Dictionary of restrictions attached to document
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=CONTENT,
            api_query=api_query,
        ):
            documents = response.get("results", [])
            attachment_count = 0
            for document in documents:
                updated_time = document["history"]["lastUpdated"]["when"]
                if document.get("children").get("attachment"):
                    attachment_count = document["children"]["attachment"]["size"]
                document_url = os.path.join(
                    self.confluence_client.host_url, document["_links"]["webui"][1:]
                )
                yield {
                    "_id": document["id"],
                    "type": document["type"],
                    "_timestamp": updated_time,
                    "title": document["title"],
                    "space": document["space"]["name"],
                    "body": document["body"]["storage"]["value"],
                    "url": document_url,
                }, attachment_count, document.get("space", {}).get(
                    "permissions", []
                ), document.get(
                    "restrictions", {}
                ).get(
                    "read", {}
                ).get(
                    "restrictions", {}
                )

    async def fetch_attachments(
        self, content_id, parent_name, parent_space, parent_type
    ):
        """Fetches all the attachments present in the given content (pages and blog posts)

        Args:
            content_id (str): Unique identifier of Confluence content (pages and blogposts)
            parent_name (str): Page/Blog post where the attachment is present
            parent_space (str): Confluence Space where the attachment is present
            parent_type (str): Type of the parent (Blog post or Page)

        Yields:
            Dictionary: Confluence attachment on the given page or blog post
            String: Download link to get the content of the attachment
        """
        async for response in self.confluence_client.paginated_api_call(
            url_name=ATTACHMENT,
            api_query=ATTACHMENT_QUERY,
            id=content_id,
        ):
            attachments = response.get("results", [])
            for attachment in attachments:
                attachment_url = os.path.join(
                    self.confluence_client.host_url,
                    attachment["_links"]["webui"][1:],
                )
                yield {
                    "type": attachment["type"],
                    "title": attachment["title"],
                    "_id": attachment["id"],
                    "space": parent_space,
                    parent_type: parent_name,
                    "_timestamp": attachment["version"]["when"],
                    "size": attachment["extensions"]["fileSize"],
                    "url": attachment_url,
                }, attachment["_links"]["download"]

    async def search_by_query(self, query):
        async for response in self.confluence_client.paginated_api_call(
            url_name=SEARCH,
            query=f"{query}&{SEARCH_QUERY}",
        ):
            results = response.get("results", [])
            for entity in results:
                # entity can be space or content
                entity_details = entity.get(SPACE) or entity.get(CONTENT)

                if (
                    entity_details["type"] == "attachment"
                    and entity_details["container"].get("title") is None
                ):
                    continue

                document = {
                    "_id": entity_details.get("id"),
                    "title": entity.get("title"),
                    "_timestamp": entity.get("lastModified"),
                    "body": entity.get("excerpt"),
                    "type": entity.get("entityType"),
                    "url": os.path.join(
                        self.confluence_client.host_url, entity.get("url")[1:]
                    ),
                }
                download_url = None
                if document["type"] == "content":
                    document.update(
                        {
                            "type": entity_details.get("type"),
                            "space": entity_details.get("space", {}).get("name"),
                        }
                    )

                    if document["type"] == "attachment":
                        container_type = entity_details.get("container", {}).get("type")
                        container_title = entity_details.get("container", {}).get(
                            "title"
                        )
                        file_size = entity_details.get("extensions", {}).get("fileSize")
                        document.update(
                            {"size": file_size, container_type: container_title}
                        )
                        # Removing body as attachment will be downloaded lazily
                        document.pop("body")
                        download_url = entity_details.get("_links", {}).get("download")

                yield document, download_url

    async def download_attachment(self, url, attachment, timestamp=None, doit=False):
        """Downloads the content of the given attachment in chunks using REST API call

        Args:
            url (str): url endpoint to download the attachment content
            attachment (dict): Dictionary containing details of the attachment
            timestamp (timestamp, optional): Timestamp of Confluence last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            Dictionary: Document of the attachment to be indexed.
        """
        file_size = int(attachment["size"])
        if not (doit and file_size):
            return

        filename = attachment["title"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {"_id": attachment["_id"], "_timestamp": attachment["_timestamp"]}
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.confluence_client.api_call,
                    url=os.path.join(self.confluence_client.host_url, url),
                ),
            ),
        )

    async def _attachment_coro(self, document, access_control):
        """Coroutine to add attachments to Queue and download content

        Args:
            document (dict): Formatted document of page/blogpost
            access_control (list): List of identities which have access to document
        """
        async for attachment, download_link in self.fetch_attachments(
            content_id=document["_id"],
            parent_name=document["title"],
            parent_space=document["space"],
            parent_type=document["type"],
        ):
            attachment = self._decorate_with_access_control(
                document=attachment, access_control=access_control
            )
            await self.queue.put(
                (  # pyright: ignore
                    attachment,
                    partial(
                        self.download_attachment,
                        download_link[1:],
                        copy(attachment),
                    ),
                )
            )
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _space_coro(self):
        """Coroutine to add spaces documents to Queue"""
        async for space, permissions in self.fetch_spaces():
            access_control = list(
                self._get_access_control_from_permission(
                    permissions=permissions, target_type=SPACE
                )
            )
            space = self._decorate_with_access_control(
                document=space, access_control=access_control
            )
            await self.queue.put((space, None))  # pyright: ignore
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _page_blog_coro(self, api_query, target_type):
        """Coroutine to add pages/blogposts to Queue

        Args:
            api_query (str): API Query Parameters for fetching page/blogpost
            target_type (str): Type of object to filter permission
        """
        async for document, attachment_count, permissions, restrictions in self.fetch_documents(
            api_query
        ):
            # Pages/Bolgpost are open to viewing or editing by default, but you can restrict either viewing or editing to certain users or groups.
            access_control = list(self._extract_identities(response=restrictions))
            if len(access_control) == 0:
                # Every space has its own independent set of permissions, managed by the space admin(s), which determine the access settings for different users and groups.
                access_control = list(
                    self._get_access_control_from_permission(
                        permissions=permissions, target_type=target_type
                    )
                )
            document = self._decorate_with_access_control(
                document=document, access_control=access_control
            )
            await self.queue.put((document, None))  # pyright: ignore
            if attachment_count > 0:
                await self.fetchers.put(
                    partial(self._attachment_coro, copy(document), access_control)
                )
                self.fetcher_count += 1
        await self.queue.put(END_SIGNAL)  # pyright: ignore

    async def _consumer(self):
        """Async generator to process entries of the queue

        Yields:
            dictionary: Documents from Confluence.
        """
        while self.fetcher_count > 0:
            _, item = await self.queue.get()
            if item == END_SIGNAL:
                self.fetcher_count -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Confluence content in async manner.

        Args:
            filtering (Filtering): Object of Class Filtering
        Yields:
            dictionary: dictionary containing meta-data of the content.
        """
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for query_info in advanced_rules:
                query = query_info.get("query")
                logger.debug(f"Fetching confluence content using custom query: {query}")
                async for document, download_link in self.search_by_query(query):
                    if download_link:
                        yield document, partial(
                            self.download_attachment,
                            download_link[1:],
                            copy(document),
                        )
                    else:
                        yield document, None

        else:
            if self.spaces == [WILDCARD]:
                configured_spaces_query = "cql=type="
            else:
                quoted_spaces = "','".join(self.spaces)
                configured_spaces_query = f"cql=space in ('{quoted_spaces}') AND type="
            await self.fetchers.put(self._space_coro)
            await self.fetchers.put(
                partial(
                    self._page_blog_coro,
                    f"{configured_spaces_query}{BLOGPOST}&{CONTENT_QUERY}",
                    BLOGPOST,
                )
            )
            await self.fetchers.put(
                partial(
                    self._page_blog_coro,
                    f"{configured_spaces_query}{PAGE}&{CONTENT_QUERY}",
                    PAGE,
                )
            )
            self.fetcher_count += 3

            async for item in self._consumer():
                yield item
            await self.fetchers.join()
