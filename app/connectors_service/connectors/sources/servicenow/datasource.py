#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""ServiceNow source module responsible to fetch documents from ServiceNow."""

import os
from enum import Enum
from functools import partial

import dateutil.parser as parser
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import (
    iso_utc,
)

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.sources.servicenow.client import (
    ENDPOINTS,
    MAX_CONCURRENT_CLIENT_SUPPORT,
    RETRIES,
    TABLE_BATCH_SIZE,
    ServiceNowClient,
)
from connectors.sources.servicenow.validator import ServiceNowAdvancedRulesValidator
from connectors.utils import (
    ConcurrentTasks,
    MemQueue,
)

QUEUE_MEM_SIZE = 25 * 1024 * 1024  # Size in Megabytes
CONCURRENT_TASKS = 1000  # Depends on total number of services and size of each service
ATTACHMENT_BATCH_SIZE = 10

RUNNING_FTEST = (
    "RUNNING_FTEST" in os.environ
)  # Flag to check if a connector is run for ftest or not.

DEFAULT_SERVICE_NAMES = {
    "sys_user": ["admin"],
    "sc_req_item": [
        "admin",
        "sn_request_read",
        "asset",
        "atf_test_designer",
        "atf_test_admin",
    ],
    "incident": ["admin", "sn_incident_read", "ml_report_user", "ml_admin", "itil"],
    "kb_knowledge": ["admin", "knowledge", "knowledge_manager", "knowledge_admin"],
    "change_request": ["admin", "sn_change_read", "itil"],
}
ACLS_QUERY = "sys_security_acl.operation=read^sys_security_acl.name={table_name}"
PUBLIC_ROLE_NAME = "public"


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_role_id(role_id):
    return prefix_identity("role_id", role_id)


class EndSignal(Enum):
    SERVICE = "SERVICE_TASK_FINISHED"
    RECORD = "RECORD_TASK_FINISHED"
    ATTACHMENT = "ATTACHMENT_TASK_FINISHED"


class ServiceNowDataSource(BaseDataSource):
    """ServiceNow"""

    name = "ServiceNow"
    service_type = "servicenow"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the ServiceNow instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """

        super().__init__(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]
        self.servicenow_client = ServiceNowClient(configuration=configuration)

        self.servicenow_mapping = {}
        self.invalid_services = []
        # Lazy per-sync cache of sys_user_role: (by_name, by_sys_id)
        self._roles_maps = None

        self.task_count = 0
        self.queue = MemQueue(maxmemsize=QUEUE_MEM_SIZE, refresh_timeout=120)
        self.fetchers = ConcurrentTasks(max_concurrency=CONCURRENT_TASKS)

    def advanced_rules_validators(self):
        return [ServiceNowAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by ServiceNow

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    @classmethod
    def get_default_configuration(cls):
        return {
            "url": {
                "label": "Service URL",
                "order": 1,
                "type": "str",
            },
            "username": {
                "label": "Username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "order": 3,
                "sensitive": True,
                "type": "str",
            },
            "services": {
                "display": "textarea",
                "label": "Comma-separated list of services",
                "order": 4,
                "tooltip": "List of services is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": "*",
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_CLIENT_SUPPORT,
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
                "tooltip": "Document level security ensures identities and permissions set in ServiceNow are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "expand_role_members": {
                "depends_on": [{"field": "use_document_level_security", "value": True}],
                "display": "toggle",
                "label": "Expand role members",
                "order": 9,
                "tooltip": "When enabled, ServiceNow role members are written individually onto each document's access control list. Disable this for large tenants to store compact role tokens on documents instead, and resolve membership during access control syncs. Changing this setting requires a full content sync and access control sync.",
                "type": "bool",
                "value": True,
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

    def _expand_role_members(self):
        """Whether roles are expanded into individual users on document ACLs.

        Default True preserves legacy behavior. When False (compact mode), documents
        receive role_id tokens and membership is resolved on identity docs.
        """
        return self.configuration.get("expand_role_members", True)

    async def _user_access_control_doc(self, user, role_ids=None):
        user_id = user.get("_id", "")
        user_name = user.get("user_name", "")
        user_email = user.get("email", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_user_name = _prefix_username(user=user_name)
        _prefixed_email = _prefix_email(email=user_email)
        access_control = [
            _prefixed_user_id,
            _prefixed_user_name,
            _prefixed_email,
        ]
        identity = {
            "user_id": _prefixed_user_id,
            "display_name": _prefixed_user_name,
            "email": _prefixed_email,
        }
        # role_ids is only set in compact mode; omit the field in legacy mode.
        if role_ids is not None:
            role_ids_list = [role_id for role_id in role_ids if role_id]
            prefixed_role_ids = sorted(
                prefixed
                for role_id in role_ids_list
                if (prefixed := _prefix_role_id(role_id)) is not None
            )
            identity["role_ids"] = prefixed_role_ids
            access_control.extend(prefixed_role_ids)
        return {
            "_id": user_id,
            "identity": identity,
            "created_at": user.get("_timestamp"),
        } | es_access_control_query(access_control=access_control)

    async def _fetch_all_users(self):
        self._logger.debug("Fetching all users.")
        async for user in self._table_data_generator(
            service_name="sys_user", params={}
        ):
            yield user

    async def _fetch_users_by_roles(self, role):
        self._logger.debug(f"Fetching users with role: {role}.")
        role_user_params = {"sysparm_query": f"role={role}"}
        async for user in self._table_data_generator(
            service_name="sys_user_has_role", params=role_user_params
        ):
            yield user

    async def _fetch_user_roles_map(self):
        """Build a map of user sys_id -> set of role sys_ids from sys_user_has_role.

        API fetches are already paginated via ``_table_data_generator``
        (``TABLE_FETCH_SIZE``). Only the user→roles map is held in memory for the
        ACL sync: O(role assignments), not O(users × documents). For large tenants
        that is tens of MB, versus the GB-scale per-document ACL expansion this
        compact mode replaces.
        """
        user_roles = {}
        async for assignment in self._table_data_generator(
            service_name="sys_user_has_role", params={}
        ):
            user_id = (assignment.get("user") or {}).get("value")
            role_id = (assignment.get("role") or {}).get("value")
            if not user_id or not role_id:
                self._logger.debug(
                    "Skipping sys_user_has_role row with missing user or role reference"
                )
                continue
            user_roles.setdefault(user_id, set()).add(role_id)
        return user_roles

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        user_roles = None
        if not self._expand_role_members():
            self._logger.info(
                "Enriching identity docs with role memberships from sys_user_has_role"
            )
            user_roles = await self._fetch_user_roles_map()

        async for user in self._fetch_all_users():
            if user_roles is None:
                yield await self._user_access_control_doc(user=user)
            else:
                user_id = user.get("_id") or user.get("sys_id")
                yield await self._user_access_control_doc(
                    user=user, role_ids=user_roles.get(user_id, set())
                )

    def _decorate_with_access_control(self, document, access_control):
        if not self._dls_enabled():
            return document
        if access_control is None:
            # Public table: omit field so DLS must_not exists grants access.
            return document
        document[ACCESS_CONTROL] = sorted(
            set(document.get(ACCESS_CONTROL, []) + access_control)
        )
        return document

    async def _remote_validation(self):
        """Validate configured services

        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """

        if self.servicenow_client.services != ["*"] and self.invalid_services == []:
            (
                self.servicenow_mapping,
                self.invalid_services,
            ) = await self.servicenow_client.filter_services(
                configured_service=self.servicenow_client.services.copy()
            )
        if self.invalid_services:
            msg = f"Services '{', '.join(self.invalid_services)}' are not available. Available services are: '{', '.join(set(self.servicenow_client.services) - set(self.invalid_services))}'"
            raise ConfigurableFieldValueError(msg)

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured services are available in ServiceNow."""

        await super().validate_config()
        await self._remote_validation()

    async def close(self):
        await self.servicenow_client.close_session()

    async def ping(self):
        """Verify the connection with ServiceNow."""

        try:
            await self.servicenow_client.ping()
            self._logger.debug("Successfully connected to the ServiceNow.")

        except Exception:
            self._logger.exception("Error while connecting to the ServiceNow.")
            raise

    def _format_doc(self, data):
        """Format document for handling empty values & type casting.

        Args:
            data (dict): Fetched record from ServiceNow.

        Returns:
            dict: Formatted document.
        """

        data = {key: value for key, value in data.items() if value}
        data.update(
            {
                "_id": data["sys_id"],
                "_timestamp": iso_utc(parser.parse(data["sys_updated_on"])),
            }
        )
        return data

    async def _fetch_attachment_metadata(self, batched_apis, table_access_control):
        try:
            async for attachments_metadata in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                for record in attachments_metadata:
                    formatted_attachment_metadata = self._format_doc(data=record)
                    serialized_attachment_metadata = self.serialize(
                        doc=formatted_attachment_metadata
                    )
                    attachment_with_access_control = self._decorate_with_access_control(
                        document=serialized_attachment_metadata,
                        access_control=table_access_control,
                    )
                    await self.queue.put(
                        (
                            attachment_with_access_control,
                            partial(
                                self.get_content,
                                attachment_with_access_control,
                            ),
                        )
                    )
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.ATTACHMENT)

    async def _attachment_metadata_producer(self, record_ids, table_access_control):
        attachment_apis = None
        try:
            attachment_apis = self.servicenow_client.get_attachment_apis(
                url=ENDPOINTS["ATTACHMENT"], ids=record_ids
            )

            for batched_apis_index in range(
                0, len(attachment_apis), ATTACHMENT_BATCH_SIZE
            ):
                batched_apis = attachment_apis[
                    batched_apis_index :   (  # noqa
                        batched_apis_index + ATTACHMENT_BATCH_SIZE
                    )
                ]
                await self.fetchers.put(
                    partial(
                        self._fetch_attachment_metadata,
                        batched_apis,
                        table_access_control,
                    )
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.exception(
                f"Skipping attachment metadata for {attachment_apis}. Exception: {exception}."
            )
            raise
        finally:
            await self.queue.put(EndSignal.RECORD)

    async def _yield_table_data(self, batched_apis):
        try:
            async for table_data in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                for record in table_data:
                    formatted_table_data = self._format_doc(data=record)
                    serialized_table_data = self.serialize(doc=formatted_table_data)
                    yield serialized_table_data
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}.",
                exc_info=True,
            )

    async def _fetch_table_data(self, batched_apis, table_access_control):
        try:
            async for table_data in self.servicenow_client.get_data(
                batched_apis=batched_apis
            ):
                record_ids = []
                for record in table_data:
                    formatted_table_data = self._format_doc(data=record)
                    serialized_table_data = self.serialize(doc=formatted_table_data)
                    record_ids.append(serialized_table_data["_id"])
                    table_data_with_access_control = self._decorate_with_access_control(
                        document=serialized_table_data,
                        access_control=table_access_control,
                    )
                    await self.queue.put(
                        (
                            table_data_with_access_control,
                            None,
                        )
                    )
                await self.fetchers.put(
                    partial(
                        self._attachment_metadata_producer,
                        record_ids,
                        table_access_control,
                    )
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping batch data for {batched_apis}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.RECORD)

    async def _get_roles_maps(self):
        """Return cached (by_name, by_sys_id) maps from sys_user_role for this sync."""
        if self._roles_maps is not None:
            return self._roles_maps
        by_name, by_sys_id = {}, {}
        async for role in self._table_data_generator(
            service_name="sys_user_role", params={}
        ):
            name = role.get("name")
            sys_id = role.get("sys_id")
            if name and sys_id:
                by_name[name] = sys_id
                by_sys_id[sys_id] = name
        self._roles_maps = (by_name, by_sys_id)
        return self._roles_maps

    async def _table_read_role_sys_ids(self, table_name):
        """Return role sys_ids that grant read on a custom (non-default) table."""
        self._logger.info(f"Fetching roles of {table_name} with read operation.")
        acl_params = {
            "sys_security_acl.operation": "read",
            "sys_security_acl.name": table_name,
            "sys_security_acl.script": "",
            "sys_security_acl.condition": "",
        }
        role_sys_ids = []
        async for acl in self._table_data_generator(
            service_name="sys_security_acl_role", params=acl_params
        ):
            role_sys_id = (acl.get("sys_user_role") or {}).get("value")
            if role_sys_id:
                role_sys_ids.append(role_sys_id)
        return role_sys_ids

    async def _fetch_access_controls_compact(self, table_name):
        """Return role_id tokens for a table, or None when the table is public."""
        roles_by_name, roles_by_sys_id = await self._get_roles_maps()
        if table_name in DEFAULT_SERVICE_NAMES:
            role_sys_ids = []
            for role_name in DEFAULT_SERVICE_NAMES.get(table_name, []):
                if role_name.lower() == PUBLIC_ROLE_NAME:
                    self._logger.info(
                        f"Compact DLS: table {table_name} has public read role — "
                        "omitting _allow_access_control on content documents"
                    )
                    return None
                role_sys_id = roles_by_name.get(role_name)
                if role_sys_id:
                    role_sys_ids.append(role_sys_id)
                else:
                    self._logger.debug(
                        f"Role '{role_name}' not found while building compact ACL "
                        f"for table {table_name}"
                    )
            return self._finalize_compact_access_control(table_name, role_sys_ids)

        role_sys_ids = await self._table_read_role_sys_ids(table_name)
        if not role_sys_ids:
            self._logger.info(
                f"Compact DLS: no read roles for table {table_name}; "
                "omitting _allow_access_control (treating as world-readable)"
            )
            return None

        resolved = []
        for role_sys_id in role_sys_ids:
            role_name = roles_by_sys_id.get(role_sys_id)
            if not role_name:
                self._logger.warning(
                    f"Skipping unknown role sys_id {role_sys_id} for table {table_name}"
                )
                continue
            if role_name.lower() == PUBLIC_ROLE_NAME:
                self._logger.info(
                    f"Compact DLS: table {table_name} has public read role — "
                    "omitting _allow_access_control on content documents"
                )
                return None
            resolved.append(role_sys_id)
        return self._finalize_compact_access_control(table_name, resolved)

    def _finalize_compact_access_control(self, table_name, role_sys_ids):
        compact_acl = sorted(
            {_prefix_role_id(role_id) for role_id in role_sys_ids if role_id}
        )
        if not compact_acl:
            self._logger.info(
                f"Compact DLS: no read roles for table {table_name}; "
                "omitting _allow_access_control (treating as world-readable)"
            )
            return None
        return compact_acl

    async def _fetch_access_controls_legacy(self, table_name):
        """Expand role members into individual user_id tokens (legacy behavior)."""
        access_control, user_roles = [], []
        roles_by_name, roles_by_sys_id = await self._get_roles_maps()
        if table_name in DEFAULT_SERVICE_NAMES.keys():
            for role in DEFAULT_SERVICE_NAMES.get(table_name, []):
                role_sys_id = roles_by_name.get(role)
                if not role_sys_id:
                    self._logger.warning(
                        f"Skipping unknown role '{role}' for table {table_name}"
                    )
                    continue
                async for user in self._fetch_users_by_roles(role_sys_id):
                    access_control.append(
                        _prefix_user_id(user_id=user.get("user", {}).get("value"))
                    )
        else:
            self._logger.info(f"Fetching roles of {table_name} with read operation.")
            acl_params = {
                "sys_security_acl.operation": "read",
                "sys_security_acl.name": table_name,
                "sys_security_acl.script": "",
                "sys_security_acl.condition": "",
            }
            async for acl in self._table_data_generator(
                service_name="sys_security_acl_role", params=acl_params
            ):
                user_roles.append(acl.get("sys_user_role", {}).get("value"))

            for role in user_roles:
                role_name = roles_by_sys_id.get(role)
                if not role_name:
                    self._logger.warning(
                        f"Skipping unknown role sys_id {role} for table {table_name}"
                    )
                    continue
                if role_name.lower() == PUBLIC_ROLE_NAME:
                    self._logger.info(
                        f"Found public role in {table_name}, Fetching all users."
                    )
                    async for user in self._fetch_all_users():
                        access_control.append(
                            _prefix_user_id(user_id=user.get("sys_id"))
                        )

                async for user in self._fetch_users_by_roles(role):
                    access_control.append(
                        _prefix_user_id(user_id=user.get("user", {}).get("value"))
                    )
        return sorted(set(access_control))

    async def _fetch_access_controls(self, table_name):
        if self._expand_role_members():
            return await self._fetch_access_controls_legacy(table_name)
        return await self._fetch_access_controls_compact(table_name)

    async def _get_batched_apis(self, service_name, params):
        table_length = await self.servicenow_client.get_table_length(
            table_name=service_name
        )
        record_apis = self.servicenow_client.get_record_apis(
            url=ENDPOINTS["TABLE"].format(table=service_name),
            params=params,
            total_count=table_length,
        )

        for batched_apis_index in range(0, len(record_apis), TABLE_BATCH_SIZE):
            batched_apis = record_apis[
                batched_apis_index : (
                    batched_apis_index + TABLE_BATCH_SIZE
                )  # noqa
            ]
            yield batched_apis

    async def _table_data_generator(self, service_name, params):
        self._logger.debug(f"Fetching {service_name} data")
        try:
            async for batched_apis in self._get_batched_apis(service_name, params):
                async for user in self._yield_table_data(batched_apis=batched_apis):
                    yield user
        except Exception as exception:
            self._logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}.",
                exc_info=True,
            )

    async def _table_data_producer(self, service_name, params, table_access_control):
        self._logger.debug(f"Fetching {service_name} data")
        try:
            async for batched_apis in self._get_batched_apis(service_name, params):
                await self.fetchers.put(
                    partial(self._fetch_table_data, batched_apis, table_access_control)
                )
                self.task_count += 1
        except Exception as exception:
            self._logger.warning(
                f"Skipping table data for {service_name}. Exception: {exception}."
            )
        finally:
            await self.queue.put(EndSignal.SERVICE)

    async def _consumer(self):
        """Consume the queue for the documents.

        Yields:
            dict: Formatted document.
        """

        while self.task_count > 0:
            _, item = await self.queue.get()

            if isinstance(item, EndSignal):
                self.task_count -= 1
            else:
                yield item

    async def get_docs(self, filtering=None):
        """Get documents from ServiceNow.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from ServiceNow.
        """

        self._logger.info("Fetching ServiceNow data")
        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            services = {rule["service"] for rule in advanced_rules}

            (
                servicenow_mapping,
                _,
            ) = await self.servicenow_client.filter_services(
                configured_service=services.copy()
            )

            for advanced_rules_index in range(0, len(advanced_rules), TABLE_BATCH_SIZE):
                batched_advanced_rules = advanced_rules[
                    advanced_rules_index : (
                        advanced_rules_index + TABLE_BATCH_SIZE
                    )  # noqa
                ]
                rules_by_service = {}
                for rule in batched_advanced_rules:
                    rules_by_service.setdefault(rule["service"], []).append(rule)

                for service_label, service_rules in rules_by_service.items():
                    table_name = servicenow_mapping[service_label]
                    table_access_control = None
                    if self._dls_enabled():
                        table_access_control = await self._fetch_access_controls(
                            table_name=table_name
                        )
                    filter_apis = await self.servicenow_client.get_filter_apis(
                        rules=service_rules, mapping=servicenow_mapping
                    )

                    await self.fetchers.put(
                        partial(
                            self._fetch_table_data,
                            filter_apis,
                            table_access_control,
                        )
                    )
                    self.task_count += 1

        else:
            if (
                self.servicenow_client.services != ["*"]
                and self.servicenow_mapping == {}
            ):
                (
                    self.servicenow_mapping,
                    self.invalid_services,
                ) = await self.servicenow_client.filter_services(
                    configured_service=self.servicenow_client.services.copy()
                )
            for service_name in (
                self.servicenow_mapping.values() or DEFAULT_SERVICE_NAMES.keys()
            ):
                table_access_control = None
                if self._dls_enabled():
                    table_access_control = await self._fetch_access_controls(
                        table_name=service_name
                    )
                await self.fetchers.put(
                    partial(
                        self._table_data_producer,
                        service_name,
                        {},
                        table_access_control,
                    )
                )
                self.task_count += 1

        async for item in self._consumer():
            yield item

        await self.fetchers.join()

    async def get_content(self, metadata, timestamp=None, doit=False):
        file_size = int(metadata["size_bytes"])
        if not (doit and file_size > 0):
            return

        filename = metadata["file_name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {"_id": metadata["id"], "_timestamp": metadata["_timestamp"]}
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.servicenow_client.download_func,
                    ENDPOINTS["DOWNLOAD"].format(sys_id=metadata["id"]),
                ),
            ),
        )
