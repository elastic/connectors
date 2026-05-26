#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from functools import cached_property
from typing import cast

from aiogoogle import AuthError
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import iso_utc

from connectors.access_control import ACCESS_CONTROL, es_access_control_query
from connectors.sources.gmail.validator import GMailAdvancedRulesValidator
from connectors.sources.shared.google import (
    GMailClient,
    GoogleDirectoryClient,
    MessageFields,
    UserFields,
    load_service_account_json,
    validate_service_account_json,
)
from connectors.utils import (
    EMAIL_REGEX_PATTERN,
    base64url_to_base64,
    validate_email_address,
)

SERVICE_ACCOUNT_CREDENTIALS_LABEL = "GMail service account JSON"
SUBJECT_LABEL = "Google Workspace admin email"
CUSTOMER_ID_LABEL = "Google customer id"

# Headers kept when trimming; everything else is dropped.
_KEPT_HEADERS = (
    "Subject",
    "From",
    "Reply-To",
    "To",
    "Cc",
    "Bcc",
    "Date",
    "Message-ID",
)

_DEFAULT_POLICY = policy.default


def _extract_body_eml(raw_base64url):
    """Trim a Gmail base64url RFC 822 message to a small .eml with only the kept
    headers and one body part (``text/plain`` preferred, ``text/html`` fallback).
    Returns standard base64 ready for ``_attachment``, ``None`` on parse failure
    (caller falls back to the legacy payload), or the input unchanged when empty.
    """
    if not raw_base64url:
        return raw_base64url

    try:
        # Gmail omits padding; appending 3 '=' covers every valid input length.
        raw_bytes = base64.urlsafe_b64decode(raw_base64url + "===")
        # typeshed declares `parsebytes` / `get_body` as `Message`, but with
        # `policy.default` they return `EmailMessage`. The casts below are
        # runtime no-ops that align pyright with reality.
        original = cast(
            EmailMessage,
            BytesParser(_class=EmailMessage, policy=_DEFAULT_POLICY).parsebytes(
                raw_bytes
            ),
        )

        rebuilt = EmailMessage(policy=_DEFAULT_POLICY)
        for header in _KEPT_HEADERS:
            if original[header] is not None:
                rebuilt[header] = original[header]

        body = cast(
            "EmailMessage | None",
            original.get_body(preferencelist=("plain", "html")),
        )
        if body is not None:
            rebuilt.set_content(
                body.get_content(),
                subtype=body.get_content_subtype(),
                charset=body.get_content_charset() or "utf-8",
            )
        else:
            # DSN / calendar invite / encrypted: headers-only output, no crash.
            rebuilt.set_content("", subtype="plain", charset="utf-8")

        return base64.b64encode(rebuilt.as_bytes()).decode("ascii")
    except Exception:
        return None


class GMailDataSource(BaseDataSource):
    """GMail"""

    name = "GMail"
    service_type = "gmail"
    advanced_rules_enabled = True
    dls_enabled = True
    incremental_sync_enabled = True

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for the GMail connector.
        Returns:
            dict: Default configuration.
        """

        return {
            "service_account_credentials": {
                "display": "textarea",
                "label": SERVICE_ACCOUNT_CREDENTIALS_LABEL,
                "sensitive": True,
                "order": 1,
                "required": True,
                "type": "str",
            },
            "subject": {
                "display": "text",
                "label": SUBJECT_LABEL,
                "order": 2,
                "required": True,
                "tooltip": "Admin account email address",
                "type": "str",
                "validations": [{"type": "regex", "constraint": EMAIL_REGEX_PATTERN}],
            },
            "customer_id": {
                "display": "text",
                "label": CUSTOMER_ID_LABEL,
                "order": 3,
                "required": True,
                "tooltip": "Google admin console -> Account -> Settings -> Customer Id",
                "type": "str",
            },
            "include_spam_and_trash": {
                "display": "toggle",
                "label": "Include spam and trash emails",
                "order": 4,
                "tooltip": "Will include spam and trash emails, when set to true.",
                "type": "bool",
                "value": False,
            },
            "include_full_raw_message": {
                "display": "toggle",
                "label": "Index full raw email (including headers)",
                "order": 5,
                "tooltip": (
                    "When disabled (default), only the email body is indexed. "
                    "Enable to keep the full raw message including routing and "
                    "authentication headers - useful for edge cases where body "
                    "extraction misses content."
                ),
                "type": "bool",
                "value": False,
            },
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 6,
                "tooltip": "Document level security ensures identities and permissions set in GMail are maintained in Elasticsearch. This enables you to restrict and personalize read-access users have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": True,
            },
        }

    async def validate_config(self):
        """Validates whether user inputs are valid or not for configuration fields.

        Raises:
            Exception: The format of service account json is invalid.
            ConfigurableFieldValueError: Subject email is invalid or Google Directory/GMail API authentication failed.

        """
        await super().validate_config()
        validate_service_account_json(
            self.configuration["service_account_credentials"], "GMail"
        )

        subject = self.configuration["subject"]

        if not validate_email_address(subject):
            msg = f"{SUBJECT_LABEL} field value needs to be a valid email address. '{subject}' is invalid."
            raise ConfigurableFieldValueError(msg)

        await self._validate_google_directory_auth()
        await self._validate_gmail_auth()

    async def _validate_gmail_auth(self):
        """
        Validates, whether the provided configuration values allow the connector to authenticate against GMail API.
        Failed authentication indicates, that either the provided credentials are incorrect or mandatory GMail API
        OAuth2 scopes are not configured.

        Raises:
            ConfigurableFieldValueError: Provided credentials are wrong or OAuth2 scopes are missing.

        """
        try:
            await self._gmail_client(self.configuration["subject"]).ping()
        except AuthError as e:
            msg = f"GMail authentication was not successful. Check the values of the following fields: '{SERVICE_ACCOUNT_CREDENTIALS_LABEL}', '{SUBJECT_LABEL}' and '{CUSTOMER_ID_LABEL}'. Also make sure that the OAuth2 scopes for GMail are setup correctly."
            raise ConfigurableFieldValueError(msg) from e

    async def _validate_google_directory_auth(self):
        """
        Validates, whether the provided configuration values allow the connector to authenticate against Google
        Directory API. Failed authentication indicates, that either the provided credentials are incorrect or mandatory
        Google Directory API OAuth2 scopes are not configured.

        Raises:
            ConfigurableFieldValueError: Provided credentials are wrong or OAuth2 scopes are missing.

        """

        try:
            await self._google_directory_client.ping()
        except AuthError as e:
            msg = f"Google Directory authentication was not successful. Check the values of the following fields: '{SERVICE_ACCOUNT_CREDENTIALS_LABEL}', '{SUBJECT_LABEL}' and '{CUSTOMER_ID_LABEL}'. Also make sure that the OAuth2 scopes for Google Directory are setup correctly."
            raise ConfigurableFieldValueError(msg) from e

    def advanced_rules_validators(self):
        return [GMailAdvancedRulesValidator()]

    def _set_internal_logger(self):
        self._google_directory_client.set_logger(self._logger)

    @cached_property
    def _service_account_credentials(self):
        service_account_credentials = load_service_account_json(
            self.configuration["service_account_credentials"], "GMail"
        )
        return service_account_credentials

    @cached_property
    def _google_directory_client(self):
        return GoogleDirectoryClient(
            json_credentials=self._service_account_credentials,
            customer_id=self.configuration["customer_id"],
            subject=self.configuration["subject"],
        )

    def _gmail_client(self, subject):
        """Instantiates a GMail client for a corresponding subject.
        Args:
            subject (str): Email address for the subject.

        Returns:
            gmail_client (GMailClient): GMail client for the corresponding subject.
        """
        gmail_client = GMailClient(
            json_credentials=self._service_account_credentials,
            customer_id=self.configuration["customer_id"],
            subject=subject,
        )
        gmail_client.set_logger(self._logger)
        return gmail_client

    async def ping(self):
        for service_name, client in [
            ("GMail", self._gmail_client(self.configuration["subject"])),
            ("Google directory", self._google_directory_client),
        ]:
            try:
                await client.ping()
                self._logger.debug(f"Successfully connected to {service_name}.")
            except Exception:
                self._logger.exception(f"Error while connecting to {service_name}.")
                raise

    def _dls_enabled(self):
        return (
            self._features is not None
            and self._features.document_level_security_enabled()
            and self.configuration["use_document_level_security"]
        )

    def access_control_query(self, access_control):
        return es_access_control_query(access_control)

    def _user_access_control_doc(self, user, access_control):
        email = user.get(UserFields.EMAIL.value)
        created_at = user.get(UserFields.CREATION_DATE.value)

        return {
            "_id": email,
            "identity": {"email": email},
            "created_at": created_at or iso_utc(),
        } | self.access_control_query(access_control)

    def _decorate_with_access_control(self, document, access_control):
        if self._dls_enabled():
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + access_control)
            )

        return document

    async def get_access_control(self):
        """Yields all users found in the Google Workspace associated with the configured service account.

        Yields:
            dict: dict representing a user

        """

        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping access control sync.")
            return

        async for user in self._google_directory_client.users():
            if user:
                access_control = [user.get(UserFields.EMAIL.value)]

                yield self._user_access_control_doc(user, access_control)

    def _message_doc(self, message):
        message_id = message.get(MessageFields.ID.value)
        raw = message.get(MessageFields.FULL_MESSAGE.value)
        timestamp = message.get(MessageFields.CREATION_DATE.value)

        if self.configuration["include_full_raw_message"]:
            # Legacy path: forward the raw email; ES attachment processor needs standard base64.
            attachment = base64url_to_base64(raw)
        else:
            # Default: trim to a header-light .eml so Tika extracts body, not headers.
            attachment = _extract_body_eml(raw)
            if attachment is None:
                if raw is not None:
                    self._logger.warning(
                        "Body extraction failed for %s; falling back to raw payload.",
                        message_id,
                    )
                attachment = base64url_to_base64(raw)

        return {
            "_id": message_id,
            "_attachment": attachment,
            "_timestamp": timestamp if timestamp is not None else iso_utc(),
        }

    async def _message_doc_with_access_control(
        self, access_control, gmail_client, message
    ):
        message_id = message.get("id")
        message_content = await gmail_client.message(message_id)
        message_content["id"] = message_id

        message_doc = self._message_doc(message_content)
        message_doc_with_access_control = self._decorate_with_access_control(
            message_doc, access_control
        )

        return message_doc_with_access_control

    async def get_docs(self, filtering=None):
        """Yields messages for all users present in the Google Workspace.
        Includes spam and trash messages, if the corresponding configuration value is set to `True`.

        Args:
            filtering (optional): Advanced filtering rules. Defaults to None.

        Yields:
            dict, partial: dict containing messages for each user,
                            partial download content function

        """

        include_spam_and_trash = self.configuration["include_spam_and_trash"]

        if include_spam_and_trash:
            self._logger.debug("Including messages from spam and trash.")
        else:
            self._logger.debug("Ignoring messages from spam and trash.")

        if _filtering_enabled(filtering):
            self._logger.debug("Fetching documents using advanced rules.")

            advanced_rules = filtering.get_advanced_rules()
            message_queries = advanced_rules.get("messages", [])

            async for user in self._google_directory_client.users():
                email = user.get(UserFields.EMAIL.value)
                access_control = [email]

                # reinitialization is needed to work around a 403 Forbidden error (see: https://issuetracker.google.com/issues/290567932)
                gmail_client = self._gmail_client(email)

                for message_query in message_queries:
                    self._logger.debug(f"Fetching messages for query: {message_query}.")

                    async for message in gmail_client.messages(
                        query=message_query, includeSpamTrash=include_spam_and_trash
                    ):
                        if not message:
                            continue

                        message_doc_with_access_control = (
                            await self._message_doc_with_access_control(
                                access_control, gmail_client, message
                            )
                        )

                        yield message_doc_with_access_control, None
        else:
            async for user in self._google_directory_client.users():
                email = user.get(UserFields.EMAIL.value)
                access_control = [email]

                # reinitialization is needed to work around a 403 Forbidden error (see: https://issuetracker.google.com/issues/290567932)
                gmail_client = self._gmail_client(email)

                async for message in gmail_client.messages(
                    includeSpamTrash=include_spam_and_trash
                ):
                    if not message:
                        continue

                    message_doc_with_access_control = (
                        await self._message_doc_with_access_control(
                            access_control, gmail_client, message
                        )
                    )

                    yield message_doc_with_access_control, None


def _filtering_enabled(filtering):
    return filtering is not None and filtering.has_advanced_rules()
