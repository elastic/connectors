#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import asyncio
import json
from functools import cached_property, partial
from urllib import parse

from connectors.sources.onedrive.constants import (
    BASE_URL,
    BATCH,
    DEFAULT_PARALLEL_CONNECTION_COUNT,
    DELTA,
    ENDPOINTS,
    FILE,
    FOLDER,
    GRAPH_API_MAX_BATCH_SIZE,
    ITEM_FIELDS,
    PING,
    RETRIES,
)
from connectors_sdk.logger import logger
from connectors_sdk.source import BaseDataSource
from connectors_sdk.utils import (
    iso_utc,
)

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.sources.onedrive.client import OneDriveClient
from connectors.sources.onedrive.validator import OneDriveAdvancedRulesValidator


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_group(group):
    return prefix_identity("group", group)


class OneDriveDataSource(BaseDataSource):
    """OneDrive"""

    name = "OneDrive"
    service_type = "onedrive"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to OneDrive

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    @cached_property
    def client(self):
        return OneDriveClient(self.configuration)

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for OneDrive

        Returns:
            dictionary: Default configuration.
        """
        return {
            "client_id": {
                "label": "Azure application Client ID",
                "order": 1,
                "type": "str",
            },
            "client_secret": {
                "label": "Azure application Client Secret",
                "order": 2,
                "sensitive": True,
                "type": "str",
            },
            "tenant_id": {
                "label": "Azure application Tenant ID",
                "order": 3,
                "type": "str",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": DEFAULT_PARALLEL_CONNECTION_COUNT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 6,
                "tooltip": "Document level security ensures identities and permissions set in OneDrive are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
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
        }

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by ServiceNow

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    def advanced_rules_validators(self):
        return [OneDriveAdvancedRulesValidator(self)]

    async def close(self):
        """Closes unclosed client session"""
        await self.client.close_session()

    async def ping(self):
        """Verify the connection with OneDrive"""
        try:
            url = parse.urljoin(BASE_URL, ENDPOINTS[PING])
            await anext(self.client.get(url=url))
            self._logger.info("Successfully connected to OneDrive")
        except Exception:
            self._logger.exception("Error while connecting to OneDrive")
            raise

    async def get_content(self, file, download_url, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            file (dict): File metadata
            download_url (str): Download URL for the file
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and file content
        """

        file_size = int(file["size"])
        if not (doit and file_size > 0):
            return

        filename = file["title"]

        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": file["_id"],
            "_timestamp": file["_timestamp"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(self.client.get, url=download_url),
            ),
        )

    def prepare_doc(self, file):
        file_info = file.get("file", {}) or {}

        modified_document = {
            "type": FILE if file.get(FILE) else FOLDER,
            "title": file.get("name"),
            "_id": file.get("id"),
            "_timestamp": file.get("lastModifiedDateTime"),
            "created_at": file.get("createdDateTime"),
            "size": file.get("size"),
            "url": file.get("webUrl"),
            "mime_type": file_info.get("mimeType"),
        }
        if self._dls_enabled():
            modified_document[ACCESS_CONTROL] = file[ACCESS_CONTROL]
        return modified_document

    def _dls_enabled(self):
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    async def _decorate_with_access_control(self, document, user_id):
        if self._dls_enabled():
            entity_permissions = await self.get_entity_permission(
                user_id=user_id, file_id=document.get("id")
            )
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + entity_permissions)
            )
        return document

    async def _user_access_control_doc(self, user):
        email = user.get("mail")
        username = user.get("userPrincipalName")

        prefixed_email = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(user.get("id"))

        prefixed_groups = set()
        user_groups = user.get("transitiveMemberOf", [])
        if len(user_groups) < 100:  # $expand param has a max of 100
            for group in user_groups:
                prefixed_groups.add(_prefix_group(group.get("id")))
        else:
            async for group in self.client.list_groups(user_id=user.get("id")):
                prefixed_groups.add(_prefix_group(group.get("id")))

        access_control = list(
            {prefixed_email, prefixed_username, prefixed_user_id}.union(prefixed_groups)
        )
        return {
            "_id": email if email else username,
            "identity": {
                "email": prefixed_email,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "created_at": user.get("createdDateTime", iso_utc()),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching all users")
        async for user in self.client.list_users(include_groups=True):
            yield await self._user_access_control_doc(user=user)

    async def get_entity_permission(self, user_id, file_id):
        if not self._dls_enabled():
            return []

        permissions = []
        async for permission in self.client.list_file_permission(
            user_id=user_id, file_id=file_id
        ):
            if identity := permission.get("grantedToV2"):
                identity_user = identity.get("user", {})
                identity_user_id = identity_user.get("id")

                if identity_user_id:
                    permissions.append(_prefix_user_id(identity_user_id))

                if identity_user and not identity_user_id:
                    self._logger.warning(
                        f"Unexpected response structure for user {user_id} for file {file_id} in `grantedToV2` response"
                    )

            if identities := permission.get("grantedToIdentitiesV2"):
                for identity in identities:
                    identity_user = identity.get("user", {})
                    identity_user_id = identity_user.get("id")

                    identity_group = identity.get("group", {})
                    identity_group_id = identity_group.get("id")

                    if identity_user_id:
                        permissions.append(_prefix_user_id(identity_user_id))

                    if identity_group_id:
                        permissions.append(_prefix_group(identity_group_id))

                    if (identity_user and not identity_user_id) or (
                        identity_group and not identity_group_id
                    ):
                        self._logger.warning(
                            f"Unexpected response structure for user {user_id} for file {file_id} in "
                            f"`grantedToIdentitiesV2` response"
                        )

        return permissions

    def _prepare_batch(self, request_id, url):
        return {"id": str(request_id), "method": "GET", "url": url, "retry_count": "0"}

    def pop_batch_requests(self, batched_apis):
        batch = batched_apis[: min(GRAPH_API_MAX_BATCH_SIZE, len(batched_apis))]
        batched_apis[:] = batched_apis[len(batch) :]
        return batch

    def lookup_request_by_id(self, requests, response_id):
        for request in requests:
            if request.get("id") == response_id:
                return request

    async def json_batching(self, batched_apis):
        while batched_apis:
            requests = self.pop_batch_requests(batched_apis)

            batch_url = parse.urljoin(BASE_URL, ENDPOINTS[BATCH])
            batch_request = json.dumps({"requests": requests})
            batch_response = {}
            async for batch_response in self.client.post(batch_url, batch_request):
                batch_response = await batch_response.json()

            for response in batch_response.get("responses", []):
                if response.get("status", 200) == 200:
                    yield response

                    if next_url := response.get("body", {}).get("@odata.nextLink"):
                        relative_url = next_url.split(BASE_URL)[1]
                        batched_apis.append(
                            self._prepare_batch(
                                request_id=response.get("id"), url=relative_url
                            )
                        )
                elif response.get("status", 200) == 429:
                    request = (
                        self.lookup_request_by_id(requests, response.get("id")) or {}
                    )
                    request["retry_count"] = str(int(request["retry_count"]) + 1)

                    if int(request["retry_count"]) > RETRIES:
                        logger.error(
                            f"Request {request} failed after {RETRIES} retries, giving up..."
                        )
                    else:
                        batched_apis.append(request)

    def send_document_to_es(self, entity, download_url):
        entity = self.prepare_doc(entity)

        if entity["type"] == FILE and download_url:
            return entity, partial(self.get_content, entity.copy(), download_url)
        else:
            return entity, None

    async def _bounbed_concurrent_tasks(
        self, items, max_concurrency, calling_func, **kwargs
    ):
        async def process_item(item, semaphore):
            async with semaphore:
                return await calling_func(item, **kwargs)

        semaphore = asyncio.Semaphore(max_concurrency)

        tasks = [process_item(item, semaphore) for item in items]

        return await asyncio.gather(*tasks)

    def build_owned_files_url(self, user):
        user_id = user.get("id")
        files_uri = f"{ENDPOINTS[DELTA].format(user_id=user_id)}?$select={ITEM_FIELDS}"

        return self._prepare_batch(request_id=user_id, url=files_uri)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch OneDrive objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            user_mail_id_map = {}
            async for user in self.client.list_users():
                user_mail_id_map[user["mail"]] = user["id"]

            for query_info in advanced_rules:
                skipped_extensions = query_info.get("skipFilesWithExtensions")
                user_mails = query_info.get("owners")
                if user_mails is None:
                    user_mails = user_mail_id_map.keys()

                pattern = query_info.get("parentPathPattern", "")

                for mail in user_mails:
                    user_id = user_mail_id_map.get(mail)
                    async for entity, download_url in self.client.get_owned_files(
                        user_id, skipped_extensions, pattern
                    ):
                        yield self.send_document_to_es(entity, download_url)
        else:
            requests = []
            async for user in self.client.list_users():
                requests.append(self.build_owned_files_url(user))

            async for response in self.json_batching(batched_apis=requests):
                files = response.get("body", {}).get("value", [])
                if entities := [file for file in files if file.get("name") != "root"]:
                    if self._dls_enabled():
                        entities = await self._bounbed_concurrent_tasks(
                            entities,
                            self.concurrent_downloads,
                            self._decorate_with_access_control,
                            user_id=response.get("id"),
                        )
                    for entity in entities:
                        download_url = entity.get("@microsoft.graph.downloadUrl")
                        yield self.send_document_to_es(entity, download_url)
