#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from functools import partial

from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import iso_utc

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.sources.dropbox.client import (
    BASE_URLS,
    ClientResponseError,
<<<<<<< HEAD
    DropBoxAdvancedRulesValidator,
=======
>>>>>>> 9f983d35 (Corrected __init__.py imports by changing to relative imports + add __all__ to prevent ruff from adding a redundant import alias on autoformat + broke out dropbox validator bc I forgot to do it)
    DropboxClient,
    EndpointName,
)
from connectors.sources.dropbox.common import (
    AUTHENTICATED_ADMIN_URL,
    ENDPOINTS,
    FILE,
    FOLDER,
    MAX_CONCURRENT_DOWNLOADS,
    PAPER,
    REQUEST_BATCH_SIZE,
    RETRY_COUNT,
    InvalidPathException,
)
<<<<<<< HEAD
=======
from connectors.sources.dropbox.validator import DropBoxAdvancedRulesValidator
>>>>>>> 9f983d35 (Corrected __init__.py imports by changing to relative imports + add __all__ to prevent ruff from adding a redundant import alias on autoformat + broke out dropbox validator bc I forgot to do it)


class DropboxDataSource(BaseDataSource):
    """Dropbox"""

    name = "Dropbox"
    service_type = "dropbox"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Dropbox

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.dropbox_client = DropboxClient(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.include_inherited_users_and_groups = self.configuration[
            "include_inherited_users_and_groups"
        ]

    def _set_internal_logger(self):
        self.dropbox_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Dropbox

        Returns:
            dictionary: Default configuration.
        """
        return {
            "path": {
                "label": "Path to fetch files/folders",
                "order": 1,
                "required": False,
                "tooltip": "Path is ignored when Advanced Sync Rules are used.",
                "type": "str",
            },
            "app_key": {
                "label": "App Key",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "app_secret": {
                "label": "App secret",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "refresh_token": {
                "label": "Refresh token",
                "sensitive": True,
                "order": 4,
                "type": "str",
            },
            "retry_count": {
                "default_value": RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 8,
                "tooltip": "Document level security ensures identities and permissions set in Dropbox are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "include_inherited_users_and_groups": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Include groups and inherited users",
                "order": 9,
                "tooltip": "Include groups and inherited users when indexing permissions. Enabling this configurable field will cause a significant performance degradation.",
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

        return self.configuration["use_document_level_security"]

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )
        return document

    async def _user_access_control_doc(self, user):
        profile = user.get("profile", {})
        email = profile.get("email")
        username = profile.get("name", {}).get("display_name")

        prefixed_email = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(profile.get("account_id"))

        prefixed_groups = set()
        for group_id in profile.get("groups", []):
            prefixed_groups.add(_prefix_group(group_id))

        access_control = list(
            {prefixed_email, prefixed_username, prefixed_user_id}.union(prefixed_groups)
        )
        return {
            "_id": email,
            "identity": {
                "email": prefixed_email,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "status": profile.get("status", {}).get(".tag"),
            "created_at": profile.get("joined_on", iso_utc()),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching members")
        async for users in self.dropbox_client.list_members():
            for user in users.get("members", []):
                yield await self._user_access_control_doc(user=user)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured path is available in Dropbox."""

        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        try:
            if self.dropbox_client.path not in ["", None]:
                await self.set_user_info()
                if self._dls_enabled():
                    _, member_id = await self.get_account_details()
                    self.dropbox_client.member_id = member_id
                    async for folder_id in self.get_team_folder_id():
                        await self.dropbox_client.check_path(folder_id=folder_id)
                else:
                    await self.dropbox_client.check_path()
        except InvalidPathException:
            raise
        except Exception as exception:
            msg = f"Error while validating path: {self.dropbox_client.path}. Error: {exception}"
            raise Exception(msg) from exception

    def advanced_rules_validators(self):
        return [DropBoxAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by dropbox

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        await self.dropbox_client.close()

    async def ping(self):
        try:
            endpoint = EndpointName.AUTHENTICATED_ADMIN.value
            await self.dropbox_client.ping(endpoint=endpoint)
            self._logger.info("Successfully connected to Dropbox")
        except ClientResponseError as exception:
            if str(exception.request_info.url) == AUTHENTICATED_ADMIN_URL:
                endpoint = EndpointName.PING.value
                await self.dropbox_client.ping(endpoint=endpoint)
                self._logger.info("Successfully connected to Dropbox")
            else:
                raise

    async def set_user_info(self):
        try:
            endpoint = EndpointName.AUTHENTICATED_ADMIN.value
            response = await self.dropbox_client.ping(endpoint=endpoint)
        except ClientResponseError as exception:
            if str(exception.request_info.url) == AUTHENTICATED_ADMIN_URL:
                endpoint = EndpointName.PING.value
                response = await self.dropbox_client.ping(endpoint=endpoint)
            else:
                raise
        _json = await response.json()

        admin_profile = _json.get("admin_profile", {}) or {}
        root_info = _json.get("root_info", {}) or {}

        self.dropbox_client.member_id = admin_profile.get("team_member_id")
        self.dropbox_client.root_namespace_id = admin_profile.get(
            "root_folder_id"
        ) or root_info.get("root_namespace_id")

    async def get_content(
        self, attachment, is_shared=False, folder_id=None, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            attachment (object): Attachment object
            is_shared (boolean, optional): Flag to check if want a content for shared file. Defaults to False.
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
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        download_func = self.download_func(is_shared, attachment, filename, folder_id)
        if not download_func:
            self._logger.warning(
                f"Skipping the file: {filename} since it is not in the downloadable format."
            )
            return

        document = {
            "_id": attachment["id"],
            "_timestamp": attachment["server_modified"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                download_func,
            ),
        )

    def download_func(self, is_shared, attachment, filename, folder_id):
        if is_shared:
            return partial(
                self.dropbox_client.download_shared_file, url=attachment["url"]
            )
        elif attachment["is_downloadable"]:
            return partial(
                self.dropbox_client.download_files,
                path=attachment["path_display"],
                folder_id=folder_id,
            )
        elif filename.split(".")[-1] == PAPER:
            return partial(
                self.dropbox_client.download_paper_files,
                path=attachment["path_display"],
                folder_id=folder_id,
            )
        else:
            return

    def _adapt_dropbox_doc_to_es_doc(self, response):
        is_file = response.get(".tag") == "file"
        if is_file and response.get("name").split(".")[-1] == PAPER:
            timestamp = response.get("client_modified")
        else:
            timestamp = response.get("server_modified") if is_file else iso_utc()
        return {
            "_id": response.get("id"),
            "type": FILE if is_file else FOLDER,
            "name": response.get("name"),
            "file_path": response.get("path_display"),
            "size": response.get("size") if is_file else 0,
            "_timestamp": timestamp,
        }

    def _adapt_dropbox_shared_file_doc_to_es_doc(self, response):
        return {
            "_id": response.get("id"),
            "type": FILE,
            "name": response.get("name"),
            "url": response.get("url"),
            "size": response.get("size"),
            "_timestamp": response.get("server_modified"),
        }

    async def _fetch_files_folders(self, path, folder_id=None):
        async for response in self.dropbox_client.get_files_folders(
            path=path, folder_id=folder_id
        ):
            for entry in response.get("entries"):
                yield self._adapt_dropbox_doc_to_es_doc(response=entry), entry

    async def _fetch_shared_files(self):
        async for response in self.dropbox_client.get_shared_files():
            for entry in response.get("entries"):
                async for metadata in self.dropbox_client.get_received_file_metadata(
                    url=entry["preview_url"]
                ):
                    json_metadata = await metadata.json()
                    yield (
                        self._adapt_dropbox_shared_file_doc_to_es_doc(
                            response=json_metadata
                        ),
                        json_metadata,
                    )

    async def advanced_sync(self, rule):
        async for response in self.dropbox_client.search_files_folders(rule=rule):
            for entry in response.get("matches"):
                data = entry.get("metadata", {}).get("metadata")
                if data.get("preview_url") and not data.get("export_info"):
                    async for (
                        metadata
                    ) in self.dropbox_client.get_received_file_metadata(
                        url=data["preview_url"]
                    ):
                        json_metadata = await metadata.json()
                        yield (
                            self._adapt_dropbox_shared_file_doc_to_es_doc(
                                response=json_metadata
                            ),
                            json_metadata,
                        )
                else:
                    yield self._adapt_dropbox_doc_to_es_doc(response=data), data

    def get_group_id(self, permission, identity):
        if identity in permission:
            return permission.get(identity).get("group_id")

    def get_email(self, permission, identity):
        if identity in permission:
            return permission.get(identity).get("email")

    async def get_permission(self, permission, account_id):
        permissions = []
        if identities := permission.get("users"):
            for identity in identities:
                permissions.append(
                    _prefix_user_id(identity.get("user", {}).get("account_id"))
                )

        if identities := permission.get("invitees"):
            for identity in identities:
                if invitee_permission := self.get_email(identity, "invitee"):
                    permissions.append(_prefix_email(invitee_permission))

        if identities := permission.get("groups"):
            for identity in identities:
                if group_permission := self.get_group_id(identity, "group"):
                    permissions.append(_prefix_group(group_permission))

        if (
            not self.include_inherited_users_and_groups
            and account_id not in permissions
        ):
            permissions.append(_prefix_user_id(account_id))
        return permissions

    async def get_folder_permission(self, shared_folder_id, account_id):
        if (
            not shared_folder_id
            or shared_folder_id == self.dropbox_client.root_namespace_id
        ):
            return [account_id]

        async for permission in self.dropbox_client.list_folder_permission(
            shared_folder_id=shared_folder_id
        ):
            return await self.get_permission(
                permission=permission, account_id=account_id
            )

    async def get_file_permission_without_batching(self, file_id, account_id):
        async for (
            permission
        ) in self.dropbox_client.list_file_permission_without_batching(file_id=file_id):
            return await self.get_permission(
                permission=permission, account_id=account_id
            )

    async def get_account_details(self):
        response = await anext(
            self.dropbox_client.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.AUTHENTICATED_ADMIN.value),
                data=json.dumps(None),
            )
        )
        account_details = await response.json()
        account_id = account_details.get("admin_profile", {}).get("account_id")
        member_id = account_details.get("admin_profile", {}).get("team_member_id")
        return account_id, member_id

    async def get_permission_list(self, item_type, item, account_id):
        if item_type == FOLDER:
            shared_folder_id = item.get("shared_folder_id") or item.get(
                "parent_shared_folder_id"
            )
            return await self.get_folder_permission(
                shared_folder_id=shared_folder_id, account_id=account_id
            )
        return await self.get_file_permission_without_batching(
            file_id=item.get("id"), account_id=account_id
        )

    async def get_team_folder_id(self):
        async for folder_list in self.dropbox_client.get_team_folder_list():
            for folder in folder_list.get("team_folders", []):
                yield folder.get("team_folder_id")

    async def map_permission_with_document(self, batched_document, account_id):
        async for permissions in self.dropbox_client.list_file_permission_with_batching(
            file_ids=batched_document.keys()
        ):
            for permission in permissions:
                file_permission = (
                    await self.get_permission(
                        permission=permission["result"]["members"],
                        account_id=account_id,
                    )
                    if permission.get("result", {}).get("members", {})
                    else []
                )
                file_id = batched_document[permission["file"]]

                yield (
                    self._decorate_with_access_control(file_id[0], file_permission),
                    file_id[1],
                )

    def document_tuple(self, document, attachment, folder_id=None):
        if document.get("type") == FILE:
            if document.get("url"):
                return document, partial(
                    self.get_content,
                    attachment=attachment,
                    is_shared=True,
                    folder_id=folder_id,
                )
            else:
                return document, partial(
                    self.get_content, attachment=attachment, folder_id=folder_id
                )
        else:
            return document, None

    async def add_document_to_list(self, func, account_id, is_shared=False):
        batched_document = {}
        calling_func = func() if is_shared else func(path=self.dropbox_client.path)

        async for document, attachment in calling_func:
            if (
                self.include_inherited_users_and_groups is True
                or document.get("type") == FOLDER
            ):
                permissions = await self.get_permission_list(
                    item_type=document.get("type"),
                    item=attachment,
                    account_id=account_id,
                )
                if permissions is None:
                    permissions = []
                yield self.document_tuple(
                    document=self._decorate_with_access_control(document, permissions),
                    attachment=attachment,
                )
            else:
                if len(batched_document) == REQUEST_BATCH_SIZE:
                    async for (
                        mapped_document,
                        mapped_attachment,
                    ) in self.map_permission_with_document(
                        batched_document=batched_document, account_id=account_id
                    ):
                        yield self.document_tuple(
                            document=mapped_document,
                            attachment=mapped_attachment,
                        )
                    batched_document = {attachment["id"]: (document, attachment)}
                else:
                    batched_document[attachment["id"]] = (document, attachment)

        if len(batched_document) > 0:
            async for (
                mapped_document,
                mapped_attachment,
            ) in self.map_permission_with_document(
                batched_document=batched_document, account_id=account_id
            ):
                yield self.document_tuple(
                    document=mapped_document,
                    attachment=mapped_attachment,
                )
            batched_document = {}

    async def fetch_file_folders_with_dls(self):
        account_id, member_id = await self.get_account_details()
        self.dropbox_client.member_id = member_id
        async for mapped_document in self.add_document_to_list(
            func=self._fetch_files_folders,
            account_id=account_id,
        ):
            yield mapped_document
        async for mapped_document in self.add_document_to_list(
            func=self._fetch_shared_files,
            account_id=account_id,
            is_shared=True,
        ):
            yield mapped_document

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        await self.set_user_info()
        if self._dls_enabled():
            async for document in self.fetch_file_folders_with_dls():
                yield document

        elif filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                self._logger.debug(f"Fetching files using advanced sync rule: {rule}")

                async for document, attachment in self.advanced_sync(rule=rule):
                    yield self.document_tuple(
                        document=document,
                        attachment=attachment,
                    )
        else:
            async for document, attachment in self._fetch_files_folders(
                path=self.dropbox_client.path
            ):
                yield self.document_tuple(
                    document=document,
                    attachment=attachment,
                )

            async for document, attachment in self._fetch_shared_files():
                yield self.document_tuple(
                    document=document,
                    attachment=attachment,
                )


def _prefix_user(user):
    if not user:
        return
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_group(group):
    return prefix_identity("group", group)
