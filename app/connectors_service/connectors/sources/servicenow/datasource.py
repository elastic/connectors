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


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_username(user):
    return prefix_identity("username", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_user(email):
    """Return a 'user:<email>' identity token.

    Uses the 'user' prefix (rather than 'email') to match the identity format
    emitted by other connectors such as SharePoint and OneDrive, enabling
    consistent cross-connector document-level security (DLS) evaluation.
    """
    return prefix_identity("user", email)


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
        """Return the default connector configuration schema.

        Each key maps to a UI field definition understood by the Kibana connector
        framework.  Fields use ``depends_on`` lists to declare which other field
        value(s) must be active before the field is shown in the UI, so only the
        fields relevant to the chosen Authentication Method and OAuth Grant Type
        are ever visible to the user at one time.

        Authentication modes
        --------------------
        ``auth_method = "basic"``
            Shows: url, username, basic_auth_password, services (+ advanced fields)

        ``auth_method = "oauth"``
            Always shows: url, oauth_grant_type, client_id, client_secret,
            services (+ advanced)

            Then per grant type:

            ``oauth_grant_type = "password"``
                + oauth_username, oauth_password

            ``oauth_grant_type = "client_credentials"``
                (no extra fields)

            ``oauth_grant_type = "refresh_token"``
                + oauth_username_refresh, refresh_token

            ``oauth_grant_type = "authorization_code"``
                + client_secret, oauth_authorization_code, oauth_redirect_uri,
                  pkce_code_verifier (optional)

            ``oauth_grant_type = "jwt_bearer"``
                + jwt_private_key, jwt_subject (optional), jwt_key_id (optional),
                  jwt_algorithm
                  (client_secret is optional and not shown by default)

        Returns:
            dict: Connector configuration schema.
        """
        return {
            "url": {
                "label": "Service URL",
                "order": 1,
                "type": "str",
            },
            "auth_method": {
                "display": "dropdown",
                "label": "Authentication Method",
                "options": [
                    {"label": "OAuth 2.0", "value": "oauth"},
                    {"label": "Basic Auth (username / password)", "value": "basic"},
                ],
                "order": 2,
                "tooltip": "Use 'OAuth 2.0' for token-based authentication (recommended). Use 'Basic Auth' for simple username and password authentication.",
                "type": "str",
                "value": "oauth",
            },
            "oauth_grant_type": {
                "depends_on": [{"field": "auth_method", "value": "oauth"}],
                "display": "dropdown",
                "label": "OAuth Grant Type",
                "options": [
                    {"label": "Password", "value": "password"},
                    {"label": "Client Credentials (requires non-PDI instance)", "value": "client_credentials"},
                    {"label": "Refresh Token", "value": "refresh_token"},
                    {"label": "Authorization Code", "value": "authorization_code"},
                    {"label": "JWT Bearer", "value": "jwt_bearer"},
                ],
                "order": 3,
                "tooltip": (
                    "Select the OAuth 2.0 grant type that matches your ServiceNow configuration. "
                    "'Password' works on all instances including PDIs. "
                    "'Client Credentials' is machine-to-machine (non-PDI only). "
                    "'Refresh Token' exchanges a long-lived refresh token. "
                    "'Authorization Code' completes the browser-based flow (supports PKCE). "
                    "'JWT Bearer' uses a signed JWT assertion (requires a private key)."
                ),
                "type": "str",
                "value": "password",
                "required": False,
            },
            "client_id": {
                "depends_on": [{"field": "auth_method", "value": "oauth"}],
                "label": "OAuth Client ID",
                "order": 4,
                "tooltip": "Required when Authentication Method is 'OAuth 2.0'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # client_secret is shown for all OAuth grant types.  Gated on
            # auth_method=oauth only (single entry) so Kibana's OR evaluation
            # doesn't cause it to bleed through to other states.
            "client_secret": {
                "depends_on": [{"field": "auth_method", "value": "oauth"}],
                "label": "OAuth Client Secret",
                "order": 5,
                "sensitive": True,
                "tooltip": (
                    "Required for Password, Client Credentials, Refresh Token, and "
                    "Authorization Code grant types. Optional for JWT Bearer — only "
                    "include if your ServiceNow OAuth application requires it."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── Username (Basic Auth) ─────────────────────────────────────────
            # depends_on uses AND semantics in the SDK: a field is shown only
            # when ALL listed conditions are simultaneously true.  Because
            # "basic" and "oauth" are mutually exclusive auth_method values,
            # username for Basic Auth and username for OAuth must be separate
            # field keys.
            "username": {
                "depends_on": [{"field": "auth_method", "value": "basic"}],
                "label": "Username",
                "order": 6,
                "tooltip": "Required when Authentication Method is 'Basic Auth'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── Basic Auth password ──────────────────────────────────────────
            "basic_auth_password": {
                "depends_on": [{"field": "auth_method", "value": "basic"}],
                "label": "Password",
                "order": 7,
                "sensitive": True,
                "tooltip": "Required when Authentication Method is 'Basic Auth'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── OAuth Username (Password grant) ──────────────────────────────
            # depends_on AND semantics: both conditions must be true simultaneously.
            # A single field cannot satisfy two different values for the same key,
            # so covering both Password and Refresh Token grant types requires two
            # separate field entries — one per grant type.
            "oauth_username": {
                "depends_on": [
                    {"field": "auth_method", "value": "oauth"},
                    {"field": "oauth_grant_type", "value": "password"},
                ],
                "label": "Username",
                "order": 8,
                "tooltip": "Required when OAuth Grant Type is 'Password'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── OAuth Username (Refresh Token grant) ─────────────────────────
            "oauth_username_refresh": {
                "depends_on": [
                    {"field": "auth_method", "value": "oauth"},
                    {"field": "oauth_grant_type", "value": "refresh_token"},
                ],
                "label": "Username",
                "order": 8,
                "tooltip": "Required when OAuth Grant Type is 'Refresh Token'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── OAuth Password grant ─────────────────────────────────────────
            # AND semantics: both conditions must hold simultaneously.
            # oauth_grant_type alone is not sufficient because its default value
            # is "password", which causes this field to bleed through to Basic
            # Auth mode where oauth_grant_type is hidden but its stored value
            # is still "password".
            "oauth_password": {
                "depends_on": [
                    {"field": "auth_method", "value": "oauth"},
                    {"field": "oauth_grant_type", "value": "password"},
                ],
                "label": "Password",
                "order": 9,
                "sensitive": True,
                "tooltip": "Required when OAuth Grant Type is 'Password'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── Refresh Token grant ──────────────────────────────────────────
            # Gating on oauth_grant_type alone is safe here: "refresh_token" is
            # never the default value of oauth_grant_type, so it cannot bleed
            # through to Basic Auth mode.
            "refresh_token": {
                "depends_on": [{"field": "oauth_grant_type", "value": "refresh_token"}],
                "label": "Refresh Token",
                "order": 10,
                "sensitive": True,
                "tooltip": "Required when OAuth Grant Type is 'Refresh Token'.",
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── Authorization Code grant ─────────────────────────────────────
            "oauth_authorization_code": {
                "depends_on": [{"field": "oauth_grant_type", "value": "authorization_code"}],
                "label": "Authorization Code",
                "order": 10,
                "sensitive": True,
                "tooltip": (
                    "The one-time authorization code returned by ServiceNow after the "
                    "user completes the browser-based consent flow. Required when "
                    "OAuth Grant Type is 'Authorization Code'."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            "oauth_redirect_uri": {
                "depends_on": [{"field": "oauth_grant_type", "value": "authorization_code"}],
                "label": "Redirect URI",
                "order": 11,
                "tooltip": (
                    "The redirect URI registered in ServiceNow for this OAuth application. "
                    "Must exactly match the URI used during the authorization flow."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            "pkce_code_verifier": {
                "depends_on": [{"field": "oauth_grant_type", "value": "authorization_code"}],
                "label": "PKCE Code Verifier",
                "order": 12,
                "sensitive": True,
                "tooltip": (
                    "Optional. The PKCE code verifier used when generating the "
                    "authorization request. Leave blank if PKCE was not used."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            # ── JWT Bearer grant ─────────────────────────────────────────────
            "jwt_private_key": {
                "depends_on": [{"field": "oauth_grant_type", "value": "jwt_bearer"}],
                "display": "textarea",
                "label": "JWT Private Key (PEM)",
                "order": 13,
                "sensitive": True,
                "tooltip": (
                    "PEM-encoded RSA or EC private key used to sign the JWT assertion. "
                    "Required when OAuth Grant Type is 'JWT Bearer'. "
                    "Paste the full key including the -----BEGIN ... KEY----- header and footer."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            "jwt_subject": {
                "depends_on": [{"field": "oauth_grant_type", "value": "jwt_bearer"}],
                "label": "JWT Subject (sub)",
                "order": 14,
                "tooltip": (
                    "Optional. The 'sub' claim in the JWT assertion. "
                    "Defaults to the OAuth Client ID when left blank."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            "jwt_key_id": {
                "depends_on": [{"field": "oauth_grant_type", "value": "jwt_bearer"}],
                "label": "JWT Key ID (kid)",
                "order": 15,
                "tooltip": (
                    "Optional. The 'kid' header in the JWT, used to identify the "
                    "signing key on the ServiceNow side. Leave blank if not required."
                ),
                "type": "str",
                "value": "",
                "required": False,
            },
            "jwt_algorithm": {
                "depends_on": [{"field": "oauth_grant_type", "value": "jwt_bearer"}],
                "display": "dropdown",
                "label": "JWT Signing Algorithm",
                "options": [
                    {"label": "RS256 (RSA, recommended)", "value": "RS256"},
                    {"label": "RS384", "value": "RS384"},
                    {"label": "RS512", "value": "RS512"},
                    {"label": "ES256 (ECDSA)", "value": "ES256"},
                    {"label": "ES384", "value": "ES384"},
                    {"label": "ES512", "value": "ES512"},
                ],
                "order": 16,
                "tooltip": "Algorithm used to sign the JWT assertion. RS256 is recommended.",
                "type": "str",
                "value": "RS256",
                "required": False,
            },
            "services": {
                "display": "textarea",
                "label": "Comma-separated list of services",
                "order": 17,
                "tooltip": "List of services is ignored when Advanced Sync Rules are used.",
                "type": "list",
                "value": "*",
            },
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 18,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_CLIENT_SUPPORT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 19,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 20,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 21,
                "tooltip": "Document level security ensures identities and permissions set in ServiceNow are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
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

    async def _user_access_control_doc(self, user):
        """Build an Elasticsearch access-control document for a ServiceNow user.

        Emits four identity tokens so that DLS queries can match the user
        regardless of which token format was indexed by a peer connector:
          - user_id:<sys_id>      (ServiceNow internal ID)
          - username:<user_name>  (login name)
          - email:<email>         (RFC 5321 address)
          - user:<email>          (cross-connector format; matches SharePoint / OneDrive)

        Args:
            user (dict): A user record as returned by _fetch_all_users().

        Returns:
            dict: Access-control document ready for bulk indexing.
        """
        user_id = user.get("_id", "")
        user_name = user.get("user_name", "")
        user_email = user.get("email", "")

        _prefixed_user_id = _prefix_user_id(user_id=user_id)
        _prefixed_user_name = _prefix_username(user=user_name)
        _prefixed_email = _prefix_email(email=user_email)
        _prefixed_user = _prefix_user(user_email)  # cross-connector format
        return {
            "_id": user_id,
            "identity": {
                "user_id": _prefixed_user_id,
                "display_name": _prefixed_user_name,
                "email": _prefixed_email,
            },
            "created_at": user.get("_timestamp"),
        } | es_access_control_query(
            access_control=[_prefixed_user_id, _prefixed_user_name, _prefixed_email, _prefixed_user]
        )

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

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        async for user in self._fetch_all_users():
            yield await self._user_access_control_doc(user=user)

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
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

    async def _fetch_access_controls(self, table_name):
        access_control, user_roles, roles = [], [], {}

        # Build a sys_id -> email map from the full user list so role-based
        # lookups (which only return a sys_id reference) can emit user:<email>
        # tokens that match the format used by other connectors (SharePoint, OneDrive).
        user_email_map = {}
        async for u in self._fetch_all_users():
            user_email_map[u.get("sys_id")] = u.get("email", "")

        if table_name in DEFAULT_SERVICE_NAMES.keys():
            async for role in self._table_data_generator(
                service_name="sys_user_role", params={}
            ):
                roles[role.get("name")] = role.get("sys_id")

            for role in DEFAULT_SERVICE_NAMES.get(table_name, []):
                role_sys_id = roles.get(role)
                if role_sys_id is None:
                    # Role defined in DEFAULT_SERVICE_NAMES but absent from the
                    # instance's sys_user_role table — skip rather than KeyError.
                    self._logger.warning(
                        f"Role '{role}' not found in sys_user_role for table '{table_name}'. Skipping."
                    )
                    continue
                async for user in self._fetch_users_by_roles(role_sys_id):
                    uid = user.get("user", {}).get("value")
                    # Prefer user:<email> for cross-connector DLS consistency;
                    # fall back to user_id:<sys_id> when email is unavailable.
                    email = user_email_map.get(uid, "")
                    if email:
                        access_control.append(_prefix_user(email))
                    else:
                        access_control.append(_prefix_user_id(uid))
        else:
            async for role in self._table_data_generator(
                service_name="sys_user_role", params={}
            ):
                roles[role.get("sys_id")] = role.get("name")

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
                if roles.get(role).lower() == "public":
                    self._logger.info(
                        f"Found public role in {table_name}, Fetching all users."
                    )
                    async for user in self._fetch_all_users():
                        uid = user.get("sys_id")
                        # Prefer user:<email>; fall back to user_id:<sys_id>.
                        email = user_email_map.get(uid, "")
                        if email:
                            access_control.append(_prefix_user(email))
                        else:
                            access_control.append(_prefix_user_id(uid))

                async for user in self._fetch_users_by_roles(role):
                    uid = user.get("user", {}).get("value")
                    # Prefer user:<email>; fall back to user_id:<sys_id>.
                    email = user_email_map.get(uid, "")
                    if email:
                        access_control.append(_prefix_user(email))
                    else:
                        access_control.append(_prefix_user_id(uid))
        return list(set(access_control))

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
                filter_apis = await self.servicenow_client.get_filter_apis(
                    rules=batched_advanced_rules, mapping=servicenow_mapping
                )

                await self.fetchers.put(
                    partial(self._fetch_table_data, filter_apis, [])
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
                table_access_control = []
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
