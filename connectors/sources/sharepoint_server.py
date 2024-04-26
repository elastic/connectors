#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SharePoint source module responsible to fetch documents from SharePoint Server.
"""
import os
from functools import partial
from urllib.parse import quote

import aiohttp
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    iso_utc,
    ssl_context,
)

RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
RETRIES = 3
TOP = 5000
PING = "ping"
SITES = "sites"
LISTS = "lists"
ATTACHMENT = "attachment"
DRIVE_ITEM = "drive_item"
LIST_ITEM = "list_item"
USERS = "users"
ADMIN_USERS = "admin_users"
ROLES = "roles"
UNIQUE_ROLES = "unique_roles"
UNIQUE_ROLES_FOR_ITEM = "unique_roles_for_item"
ROLES_BY_TITLE_FOR_LIST = "roles_by_title_for_list"
ROLES_BY_TITLE_FOR_ITEM = "roles_by_title_for_item"
ATTACHMENT_DATA = "attachment_data"
DOCUMENT_LIBRARY = "document_library"
SELECTED_FIELDS = "WikiField, Modified,Id,GUID,File,Folder"
VIEW_ITEM_MASK = 0x1  # View items in lists, documents in document libraries, and Web discussion comments.
VIEW_PAGE_MASK = 0x20000  # View pages in a Site.

READER = 2
CONTRIBUTOR = 3
WEB_DESIGNER = 4
ADMINISTRATOR = 5
EDITOR = 6
REVIEWER = 7
SYSTEM = 0xFF
VIEW_ROLE_TYPES = [
    READER,
    CONTRIBUTOR,
    WEB_DESIGNER,
    ADMINISTRATOR,
    EDITOR,
    REVIEWER,
    SYSTEM,
]


URLS = {
    PING: "{host_url}{parent_site_url}_api/web/webs",
    SITES: "{host_url}{parent_site_url}/_api/web/webs?$skip={skip}&$top={top}",
    LISTS: "{host_url}{parent_site_url}/_api/web/lists?$skip={skip}&$top={top}&$expand=RootFolder&$filter=(Hidden eq false)",
    ATTACHMENT: "{host_url}{value}/_api/web/GetFileByServerRelativeUrl('{file_relative_url}')/$value",
    DRIVE_ITEM: "{host_url}{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$select={selected_field}&$expand=File,Folder&$top={top}",
    LIST_ITEM: "{host_url}{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$expand=AttachmentFiles&$select=*,FileRef",
    ATTACHMENT_DATA: "{host_url}{parent_site_url}/_api/web/getfilebyserverrelativeurl('{file_relative_url}')",
    USERS: "{host_url}{parent_site_url}/_api/web/siteusers?$skip={skip}&$top={top}",
    ADMIN_USERS: "{host_url}{parent_site_url}/_api/web/siteusers?$skip={skip}&$top={top}&$filter=IsSiteAdmin eq true",
    ROLES: "{host_url}{parent_site_url}/_api/web/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
    UNIQUE_ROLES: "{host_url}{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/HasUniqueRoleAssignments",
    ROLES_BY_TITLE_FOR_LIST: "{host_url}{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
    UNIQUE_ROLES_FOR_ITEM: "{host_url}{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/HasUniqueRoleAssignments",
    ROLES_BY_TITLE_FOR_ITEM: "{host_url}{parent_site_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/roleassignments?$expand=Member/users,RoleDefinitionBindings&$skip={skip}&$top={top}",
}
SCHEMA = {
    SITES: {
        "title": "Title",
        "url": "Url",
        "_id": "Id",
        "server_relative_url": "ServerRelativeUrl",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LISTS: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_id": "Id",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    DOCUMENT_LIBRARY: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_id": "Id",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LIST_ITEM: {
        "title": "Title",
        "author_id": "AuthorId",
        "editor_id": "EditorId",
        "creation_time": "Created",
        "_timestamp": "Modified",
    },
    DRIVE_ITEM: {
        "title": "Name",
        "creation_time": "TimeCreated",
        "_timestamp": "TimeLastModified",
    },
}


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_login_name(name):
    return prefix_identity("login_name", name)


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_group_name(group_name):
    return prefix_identity("group_name", group_name)


def _get_login_name(raw_login_name):
    if raw_login_name:
        parts = raw_login_name.split("|")
        return parts[-1]
    return None


class SharepointServerClient:
    """SharePoint client to handle API calls made to SharePoint"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.host_url = self.configuration["host_url"]
        self.certificate = self.configuration["ssl_ca"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.retry_count = self.configuration["retry_count"]

        self.site_collections_path = []
        for site in self.configuration["site_collections"]:
            if site != "/":
                site = f"/sites/{site}/"
            self.site_collections_path.append(site)

        self.session = None
        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Generate base client session using configuration fields

        Returns:
            ClientSession: Base client session.
        """
        if self.session:
            return self.session
        self._logger.info("Generating aiohttp Client Session...")
        request_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore

        self.session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(
                login=self.configuration["username"],
                password=self.configuration["password"],
            ),  # pyright: ignore
            headers=request_headers,
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    def format_url(
        self,
        relative_url,
        list_item_id=None,
        content_type_id=None,
        is_list_item_has_attachment=False,
    ):
        if is_list_item_has_attachment:
            return (
                self.host_url
                + quote(relative_url)
                + "/DispForm.aspx?ID="
                + list_item_id
                + "&Source="
                + self.host_url
                + quote(relative_url)
                + "/AllItems.aspx&ContentTypeId="
                + content_type_id
            )
        else:
            return self.host_url + quote(relative_url)

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()  # pyright: ignore
        self.session = None

    async def _api_call(self, url_name, url="", **url_kwargs):
        """Make an API call to the SharePoint Server

        Args:
            url_name (str): SharePoint url name to be executed.
            url(str, optional): Paginated url for drive and list items. Defaults to "".
            url_kwargs (dict): Url kwargs to format the query.
        Raises:
            exception: An instance of an exception class.

        Yields:
            data: API response.
        """
        retry = 1
        # If pagination happens for list and drive items then next pagination url comes in response which will be passed in url field.
        if url == "":
            url = URLS[url_name].format(**url_kwargs)

        headers = None

        while True:
            try:
                async with self._get_session().get(  # pyright: ignore
                    url=url,
                    ssl=self.ssl_ctx,  # pyright: ignore
                    headers=headers,
                ) as result:
                    if url_name == ATTACHMENT:
                        yield result
                    else:
                        yield await result.json()
                    break
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close_session()
                if retry > self.retry_count:
                    break
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                retry += 1

                await self._sleeps.sleep(RETRY_INTERVAL**retry)

    async def _fetch_data_with_next_url(
        self, site_url, list_id, param_name, selected_field=""
    ):
        """Invokes a GET call to the SharePoint Server for calling list and drive item API.

        Args:
            site_url(string): site url to the SharePoint farm.
            list_id(string): Id of list item or drive item.
            param_name(string): parameter name whether it is DRIVE_ITEM, LIST_ITEM.
            selected_field(string): Select query parameter for drive item.
        Yields:
            Response of the GET call.
        """
        next_url = ""
        while True:
            if next_url != "":
                async for response in self._api_call(
                    url_name=param_name,
                    url=next_url,
                ):
                    yield response.get("value", [])  # pyright: ignore

                    next_url = response.get("odata.nextLink", "")
            else:
                async for response in self._api_call(
                    url_name=param_name,
                    parent_site_url=site_url,
                    list_id=list_id,
                    top=TOP,
                    host_url=self.host_url,
                    selected_field=selected_field,
                ):
                    yield response.get("value", [])  # pyright: ignore

                    next_url = response.get("odata.nextLink", "")
            if next_url == "":
                break

    async def _fetch_data_with_query(self, site_url, param_name, **kwargs):
        """Invokes a GET call to the SharePoint Server for calling site and list API.

        Args:
            site_url(string): site url to the SharePoint farm.
            param_name(string): parameter name whether it is SITES, LISTS.
        Yields:
            Response of the GET call.
        """
        skip = 0
        response_result = []
        while True:
            async for response in self._api_call(
                url_name=param_name,
                parent_site_url=site_url,
                skip=skip,
                top=TOP,
                host_url=self.host_url,
                **kwargs,
            ):
                response_result = response.get("value", [])  # pyright: ignore
                yield response_result

                skip += TOP
            if len(response_result) < TOP:
                break

    async def get_sites(self, site_url):
        """Get sites from SharePoint Server

        Args:
            site_url(string): Parent site relative path.
        Yields:
            site_server_url(string): Site path.
        """
        async for sites_data in self._fetch_data_with_query(
            site_url=site_url, param_name=SITES
        ):
            for data in sites_data:
                async for sub_site in self.get_sites(  # pyright: ignore
                    site_url=data["ServerRelativeUrl"]
                ):
                    yield sub_site
                yield data

    async def get_lists(self, site_url):
        """Get site lists from SharePoint Server

        Args:
            site_url(string): Parent site relative path.
        Yields:
            list_data(string): Response of list API call
        """
        async for list_data in self._fetch_data_with_query(
            site_url=site_url, param_name=LISTS
        ):
            for site_list in list_data:
                yield site_list

    async def get_attachment(self, site_url, file_relative_url):
        """Execute the call for fetching attachment metadata

        Args:
            site_url(string): Parent site relative path
            file_relative_url(string): Relative url of file
        Returns:
            attachment_data(dictionary): Attachment metadata
        """
        return await anext(
            self._api_call(
                url_name=ATTACHMENT_DATA,
                host_url=self.host_url,
                parent_site_url=site_url,
                file_relative_url=file_relative_url,
            )
        )

    def verify_filename_for_extraction(self, filename, relative_url):
        attachment_extension = list(os.path.splitext(filename))
        if "" in attachment_extension:
            attachment_extension.remove("")
        if "." not in filename:
            self._logger.warning(
                f"Files without extension are not supported by TIKA, skipping {filename}."
            )
            return
        if attachment_extension[-1].lower() not in TIKA_SUPPORTED_FILETYPES:
            return
        return relative_url

    async def get_list_items(self, list_id, site_url, server_relative_url, **kwargs):
        """This method fetches items from all the lists in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
        Yields:
            dictionary: dictionary containing meta-data of the list item.
        """
        file_relative_url = None
        async for list_items_data in self._fetch_data_with_next_url(
            site_url=site_url, list_id=list_id, param_name=LIST_ITEM
        ):
            for result in list_items_data:
                if not result.get("Attachments"):
                    url = self.format_url(
                        relative_url=server_relative_url,
                        list_item_id=str(result["Id"]),
                        content_type_id=result["ContentTypeId"],
                        is_list_item_has_attachment=True,
                    )
                    result["url"] = url
                    yield result, file_relative_url
                    continue

                for attachment_file in result.get("AttachmentFiles"):
                    file_relative_url = quote(
                        attachment_file.get("ServerRelativeUrl").replace(
                            "%27", "%27%27"
                        )
                    )

                    attachment_data = await self.get_attachment(
                        site_url, file_relative_url
                    )
                    result["Length"] = attachment_data.get("Length")  # pyright: ignore
                    result["_id"] = attachment_data["UniqueId"]  # pyright: ignore
                    result["url"] = self.format_url(
                        relative_url=attachment_file.get("ServerRelativeUrl")
                    )
                    result["file_name"] = attachment_file.get("FileName")
                    result["server_relative_url"] = attachment_file["ServerRelativeUrl"]

                    file_relative_url = self.verify_filename_for_extraction(
                        filename=attachment_file["FileName"],
                        relative_url=file_relative_url,
                    )

                    yield result, file_relative_url

    async def get_drive_items(self, list_id, site_url, server_relative_url, **kwargs):
        """This method fetches items from all the drives in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
            kwargs(string): Select query parameter for drive item.
        Yields:
            dictionary: dictionary containing meta-data of the drive item.
        """
        async for drive_items_data in self._fetch_data_with_next_url(
            site_url=site_url,
            list_id=list_id,
            selected_field=kwargs["selected_field"],
            param_name=DRIVE_ITEM,
        ):
            for result in drive_items_data:
                file_relative_url = None
                item_type = "Folder"

                if result.get("File", {}).get("TimeLastModified"):
                    item_type = "File"
                    filename = result["File"]["Name"]
                    file_relative_url = quote(
                        result["File"]["ServerRelativeUrl"]
                    ).replace("%27", "%27%27")
                    file_relative_url = self.verify_filename_for_extraction(
                        filename=filename, relative_url=file_relative_url
                    )
                    result["Length"] = result[item_type]["Length"]
                result["item_type"] = item_type

                yield result, file_relative_url

    async def ping(self):
        """Executes the ping call in async manner"""
        await anext(
            self._api_call(
                url_name=PING,
                parent_site_url=self.site_collections_path[0],
                host_url=self.host_url,
            )
        )

    async def fetch_users(self):
        for site_url in self.site_collections_path:
            async for users in self._fetch_data_with_query(
                site_url=site_url, param_name=USERS
            ):
                for user in users:
                    yield user

    async def site_role_assignments(self, site_url):
        async for roles in self._fetch_data_with_query(
            site_url=site_url, param_name=ROLES
        ):
            for role in roles:
                yield role

    async def site_admins(self, site_url):
        async for users in self._fetch_data_with_query(
            site_url=site_url, param_name=ADMIN_USERS
        ):
            for user in users:
                yield user

    async def site_role_assignments_using_title(self, site_url, site_list_name):
        async for roles in self._fetch_data_with_query(
            site_url=site_url,
            param_name=ROLES_BY_TITLE_FOR_LIST,
            site_list_name=site_list_name,
        ):
            for role in roles:
                yield role

    async def site_list_has_unique_role_assignments(self, site_list_name, site_url):
        role = await anext(
            self._api_call(
                url_name=UNIQUE_ROLES,
                parent_site_url=site_url,
                host_url=self.host_url,
                site_list_name=site_list_name,
            )
        )
        return role.get("value", False)

    async def site_list_item_has_unique_role_assignments(
        self, site_url, site_list_name, list_item_id
    ):
        role = await anext(
            self._api_call(
                url_name=UNIQUE_ROLES_FOR_ITEM,
                parent_site_url=site_url,
                host_url=self.host_url,
                site_list_name=site_list_name,
                list_item_id=list_item_id,
            )
        )
        return role.get("value", False)

    async def site_list_item_role_assignments(
        self, site_url, site_list_name, list_item_id
    ):
        async for roles in self._fetch_data_with_query(
            site_url=site_url,
            param_name=ROLES_BY_TITLE_FOR_ITEM,
            site_list_name=site_list_name,
            list_item_id=list_item_id,
        ):
            for role in roles:
                yield role


class SharepointServerDataSource(BaseDataSource):
    """SharePoint Server"""

    name = "SharePoint Server"
    service_type = "sharepoint_server"
    incremental_sync_enabled = True
    dls_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the SharePoint

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.sharepoint_client = SharepointServerClient(configuration=configuration)
        self.invalid_collections = []

    def _set_internal_logger(self):
        self.sharepoint_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for SharePoint

        Returns:
            dictionary: Default configuration.
        """
        return {
            "username": {
                "label": "SharePoint Server username",
                "order": 1,
                "type": "str",
            },
            "password": {
                "label": "SharePoint Server password",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "host_url": {
                "label": "SharePoint host",
                "order": 3,
                "type": "str",
            },
            "site_collections": {
                "display": "textarea",
                "label": "Comma-separated list of SharePoint site collections to index",
                "order": 4,
                "type": "list",
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
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 9,
                "tooltip": "Document level security ensures identities and permissions set in your SharePoint Server are mirrored in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "fetch_unique_list_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch unique list permissions",
                "order": 10,
                "tooltip": "Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site.",
                "type": "bool",
                "value": True,
            },
            "fetch_unique_list_item_permissions": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Fetch unique list item permissions",
                "order": 11,
                "tooltip": "Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site.",
                "type": "bool",
                "value": True,
            },
        }

    def _dls_enabled(self):
        if (
            self._features is None
            or not self._features.document_level_security_enabled()
        ):
            return False

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )

        return document

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    async def _user_access_control_doc(self, user):
        login_name = user.get("LoginName")
        prefixed_mail = _prefix_email(user.get("Email"))
        prefixed_title = _prefix_user(user.get("Title"))
        prefixed_user_id = _prefix_user_id(user.get("Id"))
        prefixed_login_name = _prefix_login_name(login_name)

        access_control = [
            prefixed_mail,
            prefixed_title,
            prefixed_user_id,
            prefixed_login_name,
        ]

        return {
            "_id": login_name,
            "identity": {
                "email": prefixed_mail,
                "title": prefixed_title,
                "user_id": prefixed_user_id,
                "login_name": prefixed_login_name,
            },
            "created_at": iso_utc(),
        } | self.access_control_query(access_control)

    async def _access_control_for_member(self, member):
        principal_type = member.get("PrincipalType")
        is_group = principal_type != 1 if principal_type else False
        user_access_control = []

        if is_group:
            group_name = member.get("Title")
            self._logger.debug(f"Detected group '{group_name}'.")
            group_id = _get_login_name(member.get("LoginName"))

            if group_id:
                user_access_control.append(_prefix_group(group_id))
            if group_name:
                user_access_control.append(_prefix_group_name(group_name))

        else:
            login_name = _get_login_name(member.get("LoginName"))
            email = member.get("Email")
            user_access_control.append(_prefix_user_id(member.get("Id")))

            if login_name:
                user_access_control.append(_prefix_login_name(login_name))

            if email:
                user_access_control.append(_prefix_email(email))
        return user_access_control

    async def get_access_control(self):
        """Yields an access control document for every user of a site.
        Note: this method will cache users and emails it has already and skip the ingestion for those.

        Yields:
             dict: dictionary representing a user access control document
        """

        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        already_seen_ids = set()

        def _already_seen(login_name):
            if login_name in already_seen_ids:
                self._logger.debug(f"Already encountered login {login_name} during this sync, skipping access control doc generation.")
                return True

            return False

        def update_already_seen(login_name):
            # We want to make sure to not add 'None' to the already seen sets
            if login_name:
                already_seen_ids.add(login_name)

        async def process_user(user):
            login_name = user.get("LoginName")
            self._logger.debug(f"Encountered login '{login_name}', generating access control doc...")

            if _already_seen(login_name):
                return None

            update_already_seen(login_name)
            if user.get("PrincipalType") == 1:
                person_access_control_doc = await self._user_access_control_doc(user)
                if person_access_control_doc:
                    return person_access_control_doc

        self._logger.info("Fetching all users")
        async for user in self.sharepoint_client.fetch_users():
            user_doc = await process_user(user)
            if user_doc:
                yield user_doc

    async def _get_access_control_from_role_assignment(self, role_assignment):
        """Extracts access control from a role assignment.

        Args:
            role_assignment (dict): dictionary representing a role assignment.

        Returns:
            access_control (list): list of usernames and dynamic group ids, which have the role assigned.

        A role can be assigned to a user directly or to a group (and therefore indirectly to the users beneath).
        If any role is assigned to a user this means at least "read" access.
        """

        def _has_limited_access(role_assignment):
            bindings = role_assignment.get("RoleDefinitionBindings", [])

            # If there is no permission information, default to restrict access
            if not bindings:
                self._logger.debug(
                    f"No RoleDefinitionBindings found for '{role_assignment.get('odata.id')}'"
                )
                return True

            # if any binding grants view access, this role assignment's member has view access
            for binding in bindings:
                base_permission_low = int(
                    binding.get("BasePermissions", {}).get("Low", "0")
                )
                role_type_kind = binding.get("RoleTypeKind", 0)
                if (
                    (base_permission_low & VIEW_ITEM_MASK)
                    or (base_permission_low & VIEW_PAGE_MASK)
                    or (role_type_kind in VIEW_ROLE_TYPES)
                ):
                    return False

            return (
                True  # no evidence of view access was found, so assuming limited access
            )

        if _has_limited_access(role_assignment):
            return []

        access_control = []
        member = role_assignment.get("Member", {})
        identity_type = member.get("odata.type", "")

        if identity_type == "SP.Group":
            users = member.get("Users", [])

            for user in users:
                access_control.extend(await self._access_control_for_member(user))
        elif identity_type == "SP.User":
            access_control.extend(await self._access_control_for_member(member))
        else:
            self._logger.debug(
                f"Skipping unique page permissions for identity type '{identity_type}'."
            )

        return access_control

    async def _site_access_control(self, site_url):
        self._logger.debug(f"Looking at site with url {site_url}")
        if not self._dls_enabled():
            return [], []

        def _is_site_admin(user):
            return user.get("IsSiteAdmin", False)

        access_control = set()
        site_admins_access_control = set()

        async for role_assignment in self.sharepoint_client.site_role_assignments(
            site_url
        ):
            member_access_control = set()
            member_access_control.update(
                await self._get_access_control_from_role_assignment(role_assignment)
            )

            if _is_site_admin(user=role_assignment.get("Member", {})):
                # These are likely in the "Owners" group for the site
                site_admins_access_control |= member_access_control

            access_control |= member_access_control

        # This fetches the "Site Collection Administrators", which is distinct from the "Owners" group of the site
        # however, both should have access to everything in the site, regardless of unique role assignments
        async for member in self.sharepoint_client.site_admins(site_url):
            site_admins_access_control.update(
                await self._access_control_for_member(member)
            )

        return list(access_control), list(site_admins_access_control)

    async def close(self):
        """Closes unclosed client session"""
        await self.sharepoint_client.close_session()

    async def _remote_validation(self):
        """Validate configured collections
        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """
        for collection in self.sharepoint_client.site_collections_path:
            is_invalid = True
            async for _ in self.sharepoint_client._api_call(
                url_name=PING,
                parent_site_url=collection,
                host_url=self.sharepoint_client.host_url,
            ):
                is_invalid = False
            if is_invalid:
                self.invalid_collections.append(collection)
        if self.invalid_collections:
            msg = (
                f"Collections {', '.join(self.invalid_collections)} are not available."
            )
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured collections are available in SharePoint."""
        await super().validate_config()
        await self._remote_validation()

    async def ping(self):
        """Verify the connection with SharePoint"""
        try:
            await self.sharepoint_client.ping()
            self._logger.debug(
                f"Successfully connected to the SharePoint via {self.sharepoint_client.host_url}"
            )
        except Exception:
            self._logger.exception(
                f"Error while connecting to the SharePoint via {self.sharepoint_client.host_url}"
            )
            raise

    def map_document_with_schema(
        self,
        document,
        item,
        document_type,
    ):
        """Prepare key mappings for documents

        Args:
            document(dictionary): Modified document
            item (dictionary): Document from SharePoint.
            document_type(string): Type of document(i.e. site,list,list_iitem, drive_item and document_library).

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        for elasticsearch_field, sharepoint_field in SCHEMA[document_type].items():
            document[elasticsearch_field] = item[sharepoint_field]

    def format_lists(
        self,
        item,
        document_type,
        admin_access_control,
        site_list_access_control,
    ):
        """Prepare key mappings for list

        Args:
            item (dictionary): Document from SharePoint.
            document_type(string): Type of document(i.e. list and document_library).
            admin_access_control(list): List of admin access control.
            site_list_access_control(list): List of access control.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": document_type}

        document["url"] = self.sharepoint_client.format_url(
            relative_url=item["RootFolder"]["ServerRelativeUrl"]
        )
        document["server_relative_url"] = item["RootFolder"]["ServerRelativeUrl"]

        self.map_document_with_schema(
            document=document, item=item, document_type=document_type
        )
        admin_access_control.extend(site_list_access_control)
        return self._decorate_with_access_control(document, admin_access_control)

    def format_sites(self, item):
        """Prepare key mappings for site

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": SITES}

        self.map_document_with_schema(document=document, item=item, document_type=SITES)
        return document

    def format_drive_item(self, item):
        """Prepare key mappings for drive items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": DRIVE_ITEM}
        item_type = item["item_type"]

        document.update(
            {  # pyright: ignore
                "_id": item["GUID"],
                "size": int(item.get("File", {}).get("Length", 0)),
                "url": self.sharepoint_client.format_url(
                    relative_url=item[item_type]["ServerRelativeUrl"]
                ),
                "server_relative_url": item[item_type]["ServerRelativeUrl"],
                "type": item_type,
            }
        )
        self.map_document_with_schema(
            document=document, item=item[item_type], document_type=DRIVE_ITEM
        )

        return document

    def format_list_item(self, item):
        """Prepare key mappings for list items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": LIST_ITEM}

        document.update(
            {  # pyright: ignore
                "_id": item["_id"] if "_id" in item.keys() else item["GUID"],
                "file_name": item.get("file_name", ""),
                "size": int(item.get("Length", 0)),
                "url": item["url"],
            }
        )
        server_url = item.get("server_relative_url")
        if server_url:
            document["server_relative_url"] = server_url

        self.map_document_with_schema(
            document=document, item=item, document_type=LIST_ITEM
        )

        return document

    async def get_lists(self, site_url, site_access_control):
        async for site_list in self.sharepoint_client.get_lists(site_url=site_url):
            has_unique_role_assignments = False
            site_list_access_control = []

            if (
                self._dls_enabled()
                and self.configuration["fetch_unique_list_permissions"]
            ):
                has_unique_role_assignments = (
                    await self.sharepoint_client.site_list_has_unique_role_assignments(
                        site_list_name=site_list["Title"], site_url=site_url
                    )
                )

                if has_unique_role_assignments:
                    self._logger.debug(
                        f"Fetching unique list permissions for list with id '{site_list['Id']}'. Ignoring parent site permissions."
                    )
                    async for role_assignment in self.sharepoint_client.site_role_assignments_using_title(
                        site_url, site_list["Title"]
                    ):
                        site_list_access_control.extend(
                            await self._get_access_control_from_role_assignment(
                                role_assignment
                            )
                        )
            if not has_unique_role_assignments:
                site_list_access_control.extend(site_access_control)
            yield site_list, site_list_access_control

    async def fetch_list_item_permission(
        self, site_url, site_list_name, list_item_id, site_access_control
    ):
        list_item_access_control = []
        has_unique_role_assignments = False

        if (
            self._dls_enabled()
            and self.configuration["fetch_unique_list_item_permissions"]
        ):
            has_unique_role_assignments = (
                await self.sharepoint_client.site_list_item_has_unique_role_assignments(
                    site_url, site_list_name, list_item_id
                )
            )
            if has_unique_role_assignments:
                self._logger.debug(
                    f"Fetching unique permissions for list item with id '{list_item_id}'. Ignoring parent site permissions."
                )
                async for role_assignment in self.sharepoint_client.site_list_item_role_assignments(
                    site_url, site_list_name, list_item_id
                ):
                    list_item_access_control.extend(
                        await self._get_access_control_from_role_assignment(
                            role_assignment
                        )
                    )
        if not has_unique_role_assignments:
            list_item_access_control.extend(site_access_control)
        return list_item_access_control

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch SharePoint objects in an async manner.

        Yields:
            dictionary: dictionary containing meta-data of the SharePoint objects.
        """

        server_relative_url = []

        for collection in self.sharepoint_client.site_collections_path:
            server_relative_url.append(collection)
            async for site_data in self.sharepoint_client.get_sites(
                site_url=collection
            ):
                server_relative_url.append(site_data["ServerRelativeUrl"])
                (
                    site_access_control,
                    site_admin_access_control,
                ) = await self._site_access_control(site_data.get("ServerRelativeUrl"))
                site_document = self._decorate_with_access_control(
                    self.format_sites(item=site_data), site_access_control
                )
                yield site_document, None

        for site_url in server_relative_url:
            (
                site_access_control,
                site_admin_access_control,
            ) = await self._site_access_control(site_url)
            async for result, site_list_access_control in self.get_lists(
                site_url=site_url, site_access_control=site_access_control
            ):
                is_site_page = False
                selected_field = ""
                # if BaseType value is 1 then it's document library else it's a list
                if result.get("BaseType") == 1:
                    if result.get("Title") == "Site Pages":
                        is_site_page = True
                        selected_field = SELECTED_FIELDS
                    yield self.format_lists(
                        item=result,
                        document_type=DOCUMENT_LIBRARY,
                        admin_access_control=site_admin_access_control.copy(),
                        site_list_access_control=site_list_access_control.copy(),
                    ), None
                    server_url = None
                    func = self.sharepoint_client.get_drive_items
                    format_document = self.format_drive_item
                else:
                    yield self.format_lists(
                        item=result,
                        document_type=LISTS,
                        admin_access_control=site_admin_access_control.copy(),
                        site_list_access_control=site_list_access_control.copy(),
                    ), None
                    server_url = result["RootFolder"]["ServerRelativeUrl"]
                    func = self.sharepoint_client.get_list_items
                    format_document = self.format_list_item

                async for item, file_relative_url in func(
                    list_id=result.get("Id"),
                    site_url=result.get("ParentWebUrl"),
                    server_relative_url=server_url,
                    selected_field=selected_field,
                ):
                    document = format_document(
                        item=item,
                    )
                    list_item_access_control = await self.fetch_list_item_permission(
                        site_url=site_url,
                        site_list_name=result.get("Title"),
                        list_item_id=item.get("Id", 0),
                        site_access_control=site_access_control.copy(),
                    )
                    # Always include site admins in list item access controls
                    list_item_access_control.extend(site_admin_access_control)

                    self._decorate_with_access_control(
                        document, list(set(list_item_access_control))
                    )

                    if file_relative_url is None:
                        yield document, None
                    else:
                        if is_site_page:
                            yield document, partial(
                                self.get_site_pages_content,
                                document,
                                item,
                            )
                        else:
                            yield document, partial(
                                self.get_content,
                                document,
                                file_relative_url,
                                site_url,
                            )

    async def get_content(
        self, document, file_relative_url, site_url, timestamp=None, doit=False
    ):
        """Get content of list items and drive items

        Args:
            document (dictionary): Modified document.
            file_relative_url (str): Relative url of file
            site_url (str): Site path of SharePoint
            timestamp (timestamp, optional): Timestamp of item last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with id, timestamp & text.
        """
        file_size = int(document["size"])
        if not (doit and file_size):
            return

        filename = (
            document["title"] if document["type"] == "File" else document["file_name"]
        )
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        file_doc = {
            "_id": document.get("id"),
            "_timestamp": document.get("_timestamp"),
        }
        return await self.download_and_extract_file(
            file_doc,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.sharepoint_client._api_call,
                    url_name=ATTACHMENT,
                    host_url=self.sharepoint_client.host_url,
                    value=site_url,
                    file_relative_url=file_relative_url,
                ),
            ),
        )

    async def get_site_pages_content(
        self, document, list_response, timestamp=None, doit=False
    ):
        """Get content of site pages for SharePoint

        Args:
            document (dictionary): Modified document.
            list_response (dict): Dictionary of list item response
            timestamp (timestamp, optional): Timestamp of item last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with id, timestamp & text.
        """
        file_size = int(document["size"])
        if not (doit and file_size):
            return

        filename = (
            document["title"] if document["type"] == "File" else document["file_name"]
        )
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        response_data = list_response["WikiField"]
        if response_data is None:
            return

        file_doc = {
            "_id": document.get("id"),
            "_timestamp": document.get("_timestamp"),
        }
        return await self.download_and_extract_file(
            file_doc,
            filename,
            file_extension,
            partial(
                self.download_func,
                response_data,
            ),
        )

    async def download_func(self, response_data):
        """This is a fake-download function
        Its only purpose is to allow response_data to be
        written to a temp file.
        This is because sharepoint server page content aren't download files,
        it instead contains a key with bytes in its response.
        """
        yield bytes(response_data, "utf-8")
