#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from functools import cached_property

import fastjsonschema
from aiogoogle import AuthError
from fastjsonschema import JsonSchemaValueException

from connectors.access_control import ACCESS_CONTROL, es_access_control_query
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.sources.google import (
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
    iso_utc,
    validate_email_address,
)

CUSTOMER_ID_LABEL = "Google customer id"

SUBJECT_LABEL = "Subject"

SERVICE_ACCOUNT_CREDENTIALS_LABEL = "GMail service account JSON"

GMAIL_API_TIMEOUT = GOOGLE_DIRECTORY_TIMEOUT = 1 * 60  # 1 min


class GMailAdvancedRulesValidator(AdvancedRulesValidator):
    MESSAGES_SCHEMA_DEFINITION = {
        "type": "array",
        "items": {"type": "string"},
        "minItems": 1,
    }

    SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {"messages": MESSAGES_SCHEMA_DEFINITION},
        "additionalProperties": False,
    }

    SCHEMA = fastjsonschema.compile(
        definition=SCHEMA_DEFINITION,
    )

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        try:
            GMailAdvancedRulesValidator.SCHEMA(advanced_rules)

            return SyncRuleValidationResult.valid_result(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES
            )
        except JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )


def _message_doc(message):
    timestamp_field = "_timestamp"

    # We're using the `_attachment` field here so the attachment processor on the ES side decodes the base64 value
    message_fields_to_es_doc_mappings = {
        MessageFields.ID: "_id",
        MessageFields.FULL_MESSAGE: "_attachment",
        MessageFields.CREATION_DATE: timestamp_field,
    }

    es_doc = {
        es_doc_field: message.get(message_field.value)
        for message_field, es_doc_field in message_fields_to_es_doc_mappings.items()
    }

    # The attachment processor cannot handle base64url encoded values (only ordinary base64)
    es_doc["_attachment"] = base64url_to_base64(es_doc["_attachment"])

    if es_doc.get(timestamp_field) is None:
        es_doc[timestamp_field] = iso_utc()

    return es_doc


def _filtering_enabled(filtering):
    return filtering is not None and filtering.has_advanced_rules()


class GMailDataSource(BaseDataSource):
    """GMail"""

    name = "GMail"
    service_type = "gmail"
    advanced_rules_enabled = True
    dls_enabled = True

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
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 5,
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
            msg = f"Subject field value needs to be a valid email address. '{subject}' is invalid."
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

    async def _message_doc_with_access_control(
        self, access_control, gmail_client, message
    ):
        message_id = message.get("id")
        message_content = await gmail_client.message(message_id)
        message_content["id"] = message_id

        message_doc = _message_doc(message_content)
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
