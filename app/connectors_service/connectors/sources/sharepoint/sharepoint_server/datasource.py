#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from functools import partial

from connectors_sdk.source import (
    CHUNK_SIZE,
    BaseDataSource,
    ConfigurableFieldValueError,
)
from connectors_sdk.utils import (
    hash_id,
    iso_utc,
)

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)

from connectors.sources.sharepoint.sharepoint_server.client import (
    SharepointServerClient,
)
from connectors.sources.sharepoint.sharepoint_server.constants import (
    BASIC_AUTH,
    NTLM_AUTH,
    RETRIES,
    IS_USER,
    VIEW_ITEM_MASK,
    VIEW_PAGE_MASK,
    VIEW_ROLE_TYPES,
    DOCUMENT_LIBRARY,
    ATTACHMENT,
    PING,
    SCHEMA,
    LISTS,
    SELECTED_FIELDS,
    SITES,
    DRIVE_ITEM,
    LIST_ITEM
)

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

    def _prefix_user(self, user):
        return prefix_identity("user", user)

    def _prefix_user_id(self, user_id):
        return prefix_identity("user_id", user_id)

    def _prefix_email(self, email):
        return prefix_identity("email", email)

    def _prefix_login_name(self, name):
        return prefix_identity("login_name", name)

    def _prefix_group(self, group):
        return prefix_identity("group", group)

    def _prefix_group_name(self, group_name):
        return prefix_identity("group_name", group_name)

    def _get_login_name(self, raw_login_name):
        if raw_login_name:
            parts = raw_login_name.split("|")
            return parts[-1]
        return None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for SharePoint

        Returns:
            dictionary: Default configuration.
        """
        return {
            "authentication": {
                "label": "Authentication mode",
                "order": 1,
                "type": "str",
                "options": [
                    {"label": "Basic", "value": BASIC_AUTH},
                    {"label": "NTLM", "value": NTLM_AUTH},
                ],
                "display": "dropdown",
                "value": BASIC_AUTH,
            },
            "username": {
                "label": "SharePoint Server username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "label": "SharePoint Server password",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "host_url": {
                "label": "SharePoint host",
                "order": 4,
                "type": "str",
            },
            "site_collections": {
                "display": "textarea",
                "label": "Comma-separated list of SharePoint site collections to index",
                "order": 5,
                "type": "list",
                "required": True,
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 6,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 7,
                "type": "str",
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 8,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 9,
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
        prefixed_mail = self._prefix_email(user.get("Email"))
        prefixed_title = self._prefix_user(user.get("Title"))
        prefixed_user_id = self._prefix_user_id(user.get("Id"))
        prefixed_login_name = self._prefix_login_name(login_name)

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
        is_group = principal_type != IS_USER if principal_type else False
        user_access_control = []

        if is_group:
            group_name = member.get("Title")
            group_id = self._get_login_name(member.get("LoginName"))
            self._logger.debug(
                f"Detected member '{group_id}' with name '{group_name}' and principal type '{principal_type}', processing as group."
            )

            if group_id:
                user_access_control.append(self._prefix_group(group_id))
            if group_name:
                user_access_control.append(self._prefix_group_name(group_name))

        else:
            login_name = self._get_login_name(member.get("LoginName"))
            email = member.get("Email")
            _id = member.get("Id")
            self._logger.debug(
                f"Detected member '{_id}' with login '{login_name}:{email}' and principal type '{principal_type}', processing as individual."
            )
            user_access_control.append(self._prefix_user_id(_id))

            if login_name:
                user_access_control.append(self._prefix_login_name(login_name))

            if email:
                user_access_control.append(self._prefix_email(email))
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
                self._logger.debug(
                    f"Already encountered login {login_name} during this sync, skipping access control doc generation."
                )
                return True

            return False

        def update_already_seen(login_name):
            # We want to make sure to not add 'None' to the already seen sets
            if login_name:
                already_seen_ids.add(login_name)

        async def process_user(user):
            login_name = user.get("LoginName")
            self._logger.debug(
                f"Encountered login '{login_name}', generating access control doc..."
            )

            if _already_seen(login_name):
                return None

            update_already_seen(login_name)
            if user.get("PrincipalType") == IS_USER:
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

        for collection in self.sharepoint_client.site_collections:
            is_invalid = True
            async for _ in self.sharepoint_client._api_call(
                url_name=PING,
                site_collections=collection,
                site_url=collection,
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
        site_url,
        list_relative_url,
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
        document = {
            "type": document_type,
            "_id": hash_id(f'{item.get("Id", "")}/{item.get("ParentWebUrl", "")}'),
        }

        document["url"] = self.sharepoint_client.format_url(
            site_url=site_url, relative_url=list_relative_url
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
        document = {
            "type": SITES,
            "_id": hash_id(f'{item.get("Id", "")}/{item.get("Url", "")}'),
            "author": item.get("Author", {}).get("LoginName", ""),
            "author_id": item.get("Author", {}).get("Id", ""),
        }

        self.map_document_with_schema(document=document, item=item, document_type=SITES)
        return document

    def format_drive_item(
        self,
        site_url,
        server_relative_url,
        item,
    ):
        """Prepare key mappings for drive items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": DRIVE_ITEM}
        item_type = item["item_type"]

        item_relative_url = self.sharepoint_client.fix_relative_url(
            server_relative_url, item[item_type]["ServerRelativeUrl"]
        )
        document.update(
            {  # pyright: ignore
                "_id": hash_id(
                    f'{item["GUID"]}/{item[item_type]["ServerRelativeUrl"]}'
                ),
                "size": int(item.get("File", {}).get("Length", 0)),
                "url": self.sharepoint_client.format_url(
                    site_url=site_url, relative_url=item_relative_url
                ),
                "server_relative_url": item[item_type]["ServerRelativeUrl"],
                "type": item_type,
                "author": item.get("Author", {}).get("Name", ""),
                "author_id": item.get("Author", {}).get("Id", ""),
                "editor": item.get("Editor", {}).get("Name", ""),
                "editor_id": item.get("Editor", {}).get("Id", ""),
            }
        )
        self.map_document_with_schema(
            document=document, item=item[item_type], document_type=DRIVE_ITEM
        )

        return document

    def format_list_item(
        self,
        item,
        site_url=None,
        server_relative_url=None,
    ):
        """Prepare key mappings for list items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": LIST_ITEM}

        document.update(
            {  # pyright: ignore
                "_id": hash_id(
                    f'{item["_id"] if "_id" in item.keys() else item["GUID"]}/{item["url"]}'
                ),
                "file_name": item.get("file_name", ""),
                "size": int(item.get("Length", 0)),
                "url": item["url"],
                "author": item.get("Author", {}).get("Name", ""),
                "editor": item.get("Editor", {}).get("Name", ""),
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
                    async for (
                        role_assignment
                    ) in self.sharepoint_client.site_role_assignments_using_title(
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
                async for (
                    role_assignment
                ) in self.sharepoint_client.site_list_item_role_assignments(
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

        sites = []

        for collection in self.sharepoint_client.site_collections:
            async for site_data in self.sharepoint_client.get_site(site_url=collection):
                sites.append(site_data)
                (
                    site_access_control,
                    site_admin_access_control,
                ) = await self._site_access_control(
                    site_url=f'{self.sharepoint_client.host_url}{site_data.get("ServerRelativeUrl")}'
                )
                site_document = self._decorate_with_access_control(
                    self.format_sites(item=site_data), site_access_control
                )
                yield site_document, None
            async for site_data in self.sharepoint_client.get_sites(
                site_url=collection
            ):
                sites.append(site_data)
                (
                    site_access_control,
                    site_admin_access_control,
                ) = await self._site_access_control(
                    site_url=f'{self.sharepoint_client.host_url}{site_data.get("ServerRelativeUrl")}'
                )
                site_document = self._decorate_with_access_control(
                    self.format_sites(item=site_data), site_access_control
                )
                yield site_document, None
        for site in sites:
            site_url = site["Url"]
            server_relative_url = site["ServerRelativeUrl"]
            (
                site_access_control,
                site_admin_access_control,
            ) = await self._site_access_control(site_url)
            async for result, site_list_access_control in self.get_lists(
                site_url=site_url, site_access_control=site_access_control
            ):
                is_site_page = False
                selected_field = "*,Author/Name,Editor/Name,Author/Id,Editor/Id"
                list_relative_url = self.sharepoint_client.fix_relative_url(
                    server_relative_url,
                    result["RootFolder"]["ServerRelativeUrl"],
                )
                # if BaseType value is 1 then it's document library else it's a list
                if result.get("BaseType") == 1:
                    if result.get("Title") == "Site Pages":
                        is_site_page = True
                        selected_field = SELECTED_FIELDS
                    yield (
                        self.format_lists(
                            item=result,
                            document_type=DOCUMENT_LIBRARY,
                            admin_access_control=site_admin_access_control.copy(),
                            site_list_access_control=site_list_access_control.copy(),
                            site_url=site_url,
                            list_relative_url=list_relative_url,
                        ),
                        None,
                    )
                    list_relative_url = None
                    func = self.sharepoint_client.get_drive_items
                    format_document = self.format_drive_item
                else:
                    yield (
                        self.format_lists(
                            item=result,
                            document_type=LISTS,
                            admin_access_control=site_admin_access_control.copy(),
                            site_list_access_control=site_list_access_control.copy(),
                            site_url=site_url,
                            list_relative_url=list_relative_url,
                        ),
                        None,
                    )
                    func = self.sharepoint_client.get_list_items
                    format_document = self.format_list_item
                async for item, file_relative_url in func(
                    list_id=result.get("Id"),
                    site_url=f"{self.sharepoint_client.host_url}{result.get('ParentWebUrl')}",
                    server_relative_url=server_relative_url,
                    list_relative_url=list_relative_url,
                    selected_field=selected_field,
                ):
                    document = format_document(
                        item=item,
                        site_url=site_url,
                        server_relative_url=server_relative_url,
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
                            yield (
                                document,
                                partial(
                                    self.get_site_pages_content,
                                    document,
                                    item,
                                ),
                            )
                        else:
                            yield (
                                document,
                                partial(
                                    self.get_content,
                                    document,
                                    file_relative_url,
                                    site_url,
                                ),
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
                self.chunked_download_func,
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

        response_data = list_response.get("CanvasContent1") or list_response.get(
            "WikiField"
        )
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

    async def chunked_download_func(self, download_func):
        """
        This provides a wrapper for chunked download funcs that
        use `response.content.iterchunked`.
        This should not be used for downloads that use other methods.
        """
        async for response in download_func():
            async for data in response.aiter_bytes(CHUNK_SIZE):
                yield data
