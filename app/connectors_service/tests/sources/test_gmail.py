#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
import email
import json
from contextlib import asynccontextmanager
from email import policy
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from aiogoogle import AuthError
from connectors_sdk.filtering.validation import Filter
from connectors_sdk.source import ConfigurableFieldValueError
from connectors_sdk.utils import Features, iso_utc
from freezegun import freeze_time

from connectors.access_control import ACCESS_CONTROL
from connectors.sources.gmail import GMailAdvancedRulesValidator, GMailDataSource
from connectors.sources.gmail.datasource import _extract_body_eml
from connectors.sources.shared.google import MessageFields, UserFields
from connectors.utils import base64url_to_base64
from tests.commons import AsyncIterator
from tests.sources.support import create_source

TIME = "2023-01-24T04:07:19"

CUSTOMER_ID = "customer_id"

SUBJECT = "subject@email_address.com"

DATE = "2023-01-24T04:07:19+00:00"

JSON_CREDENTIALS = {"project_id": "dummy123"}


def dls_feature_enabled(value):
    return value


def dls_rcf_enabled(value):
    return value


def dls_enabled(value):
    return value


@asynccontextmanager
async def create_gmail_source(
    dls_enabled=False,
    include_spam_and_trash=False,
    include_full_raw_message=False,
):
    async with create_source(
        GMailDataSource,
        service_account_credentials=json.dumps(JSON_CREDENTIALS),
        subject=SUBJECT,
        customer_id="foo",
        use_document_level_security=dls_enabled,
        include_spam_and_trash=include_spam_and_trash,
        include_full_raw_message=include_full_raw_message,
    ) as source:
        source.set_features(
            Features({"document_level_security": {"enabled": dls_enabled}})
        )
        source._service_account_credentials = MagicMock()

        yield source


class TestGMailAdvancedRulesValidator:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "advanced_rules, is_valid",
        [
            (
                # empty advanced rules (dictionary)
                {},
                True,
            ),
            (
                # empty advanced rules (array)
                [],
                True,
            ),
            (
                # string in array
                {
                    "messages": ["sent"],
                },
                True,
            ),
            (
                # multiple strings in array
                {
                    "messages": ["sent", "some other query"],
                },
                True,
            ),
            (
                # wrong type
                {
                    "messages": [1],
                },
                False,
            ),
        ],
    )
    async def test_advanced_rules_validator(self, advanced_rules, is_valid):
        validation_result = await GMailAdvancedRulesValidator().validate(advanced_rules)
        assert validation_result.is_valid == is_valid


MESSAGE_ID = 1
CREATION_DATE = "2023-01-01T13:37:00"

# Headers that Gmail attaches to every `format=raw` response and that we want gone
# from the trimmed `_attachment` payload. Used both to build realistic fixtures and
# to assert they have been stripped.
_NOISY_HEADERS = (
    "Delivered-To",
    "Received",
    "ARC-Seal",
    "ARC-Message-Signature",
    "ARC-Authentication-Results",
    "DKIM-Signature",
    "X-Google-Smtp-Source",
    "X-Received",
    "Return-Path",
    "Authentication-Results",
    "List-Unsubscribe",
    "Message-ID",
)

# A reusable block of noisy headers that mirrors what Gmail prepends to a raw message.
_NOISY_HEADER_BLOCK = (
    "Delivered-To: recipient@example.com\r\n"
    "Received: by 2002:a17:abc with SMTP id xyz; Wed, 13 May 2026 03:00:00 -0700 (PDT)\r\n"
    "X-Google-Smtp-Source: ABC123source\r\n"
    "X-Received: by 2002:a17:def; Wed, 13 May 2026 03:00:01 -0700 (PDT)\r\n"
    "ARC-Seal: i=1; a=rsa-sha256; t=1747130400; cv=none; d=google.com; s=arc-20240605\r\n"
    "ARC-Message-Signature: i=1; a=rsa-sha256; c=relaxed/relaxed; d=google.com; s=arc-20240605\r\n"
    "ARC-Authentication-Results: i=1; mx.google.com; dkim=pass header.i=@example.com\r\n"
    "Return-Path: <bounce@example.com>\r\n"
    "Authentication-Results: mx.google.com; dkim=pass header.i=@example.com\r\n"
    "DKIM-Signature: v=1; a=rsa-sha256; c=relaxed/relaxed; d=example.com; s=selector\r\n"
    "List-Unsubscribe: <mailto:unsubscribe@example.com>\r\n"
    "Message-ID: <abc123@example.com>\r\n"
)


def _to_eml_bytes(text):
    """Normalize line endings to CRLF, which is what RFC 822 expects on the wire."""
    return text.replace("\r\n", "\n").replace("\n", "\r\n").encode("utf-8")


def _b64url(raw_bytes):
    return base64.urlsafe_b64encode(raw_bytes).decode("ascii")


# Fixture 1: text/plain only with the noisy Gmail header block prepended.
_PLAIN_ONLY = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: Plain only test\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + "Content-Type: text/plain; charset=utf-8\r\n"
    + "\r\n"
    + "This is the plain text body of the message.\r\n"
)

# Fixture 2: multipart/alternative with both text/plain and text/html. Plain wins.
_ALTERNATIVE = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: Both parts\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + 'Content-Type: multipart/alternative; boundary="alt"\r\n'
    + "\r\n"
    + "--alt\r\n"
    + "Content-Type: text/plain; charset=utf-8\r\n"
    + "\r\n"
    + "Plain version of the body.\r\n"
    + "--alt\r\n"
    + "Content-Type: text/html; charset=utf-8\r\n"
    + "\r\n"
    + "<html><body>HTML version of the body.</body></html>\r\n"
    + "--alt--\r\n"
)

# Fixture 3: text/html only. Body survives as HTML in the trimmed .eml.
_HTML_ONLY = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: HTML only\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + "Content-Type: text/html; charset=utf-8\r\n"
    + "\r\n"
    + "<html><body><p>HTML only body content.</p></body></html>\r\n"
)

# Fixture 4: multipart/mixed with body + PDF attachment. Attachment dropped, body kept.
_MIXED_WITH_ATTACHMENT = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: With attachment\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + 'Content-Type: multipart/mixed; boundary="mix"\r\n'
    + "\r\n"
    + "--mix\r\n"
    + "Content-Type: text/plain; charset=utf-8\r\n"
    + "\r\n"
    + "Body with a PDF attached.\r\n"
    + "--mix\r\n"
    + 'Content-Type: application/pdf; name="doc.pdf"\r\n'
    + 'Content-Disposition: attachment; filename="doc.pdf"\r\n'
    + "Content-Transfer-Encoding: base64\r\n"
    + "\r\n"
    + "JVBERi0xLjQKJcOkw7zDtsOfCjIgMCBvYmoKPDwvVHlwZS9YT2JqZWN0Pj4KZW5kb2JqCg==\r\n"
    + "--mix--\r\n"
)

# Fixture 5: multipart/related, HTML + inline image. Image part dropped.
_RELATED_INLINE_IMAGE = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: HTML with inline image\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + 'Content-Type: multipart/related; boundary="rel"\r\n'
    + "\r\n"
    + "--rel\r\n"
    + "Content-Type: text/html; charset=utf-8\r\n"
    + "\r\n"
    + '<html><body><img src="cid:img1">Inline image body.</body></html>\r\n'
    + "--rel\r\n"
    + "Content-Type: image/png\r\n"
    + "Content-Transfer-Encoding: base64\r\n"
    + "Content-ID: <img1>\r\n"
    + "\r\n"
    + "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABAQMAAAAl21bKAAAAA1BMVEX/AAAZ4gk3AAAAAXRSTlPM\r\n"
    + "0jdYAAAACklEQVQI12NgAAAAAgAB4iG8MwAAAABJRU5ErkJggg==\r\n"
    + "--rel--\r\n"
)

# Fixture 6: RFC 2047 encoded subject (Japanese "konnichiwa"). policy.default decodes
# on read; we want to see the decoded subject in the rebuilt message.
_ENCODED_SUBJECT = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: =?UTF-8?B?44GT44KT44Gr44Gh44Gv?=\r\n"
    + "From: sender@example.com\r\n"
    + "To: recipient@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + "Content-Type: text/plain; charset=utf-8\r\n"
    + "\r\n"
    + "Body with an encoded subject.\r\n"
)

# Fixture 7: Body declared as Windows-1252. Should be re-encoded as UTF-8 in output.
# The byte 0x93 is a Windows-1252 left double quotation mark; 0x94 is the right one.
_WINDOWS_1252_BODY = (
    _to_eml_bytes(
        _NOISY_HEADER_BLOCK
        + "Subject: Windows-1252 body\r\n"
        + "From: sender@example.com\r\n"
        + "To: recipient@example.com\r\n"
        + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
        + "MIME-Version: 1.0\r\n"
        + "Content-Type: text/plain; charset=Windows-1252\r\n"
        + "\r\n"
    )
    + b"Smart quote: \x93hello\x94 world.\r\n"
)

# Fixture 8: Delivery Status Notification - no textual body part, headers only.
_DSN = _to_eml_bytes(
    _NOISY_HEADER_BLOCK
    + "Subject: Delivery Status Notification (Failure)\r\n"
    + "From: mailer-daemon@example.com\r\n"
    + "To: sender@example.com\r\n"
    + "Date: Wed, 13 May 2026 10:00:00 +0000\r\n"
    + "MIME-Version: 1.0\r\n"
    + 'Content-Type: multipart/report; report-type=delivery-status; boundary="dsn"\r\n'
    + "\r\n"
    + "--dsn\r\n"
    + "Content-Type: message/delivery-status\r\n"
    + "\r\n"
    + "Reporting-MTA: dns; mx.example.com\r\n"
    + "\r\n"
    + "Final-Recipient: rfc822; bad@example.com\r\n"
    + "Action: failed\r\n"
    + "Status: 5.1.1\r\n"
    + "--dsn--\r\n"
)


# Shared base64url fixture used by the broader sync tests so they exercise the new
# trim path with a realistic Gmail-shaped raw payload.
SAMPLE_RAW_BASE64URL = _b64url(_PLAIN_ONLY)


async def setup_messages_and_users_apis(
    patch_gmail_client, patch_google_directory_client, messages, users
):
    patch_google_directory_client.users = AsyncIterator(users)
    patch_gmail_client.messages = AsyncIterator(messages)
    patch_gmail_client.message = AsyncMock(side_effect=messages)


def _decode_attachment(attachment):
    """Decode an `_attachment` payload (standard base64) into an EmailMessage."""
    return email.message_from_bytes(base64.b64decode(attachment), policy=policy.default)


class TestGMailDataSource:
    @pytest_asyncio.fixture
    async def patch_gmail_client(self):
        with patch(
            "connectors.sources.gmail.datasource.GMailClient", return_value=AsyncMock()
        ) as mock:
            client = mock.return_value
            yield client

    @pytest_asyncio.fixture
    async def patch_google_directory_client(self):
        with patch(
            "connectors.sources.gmail.datasource.GoogleDirectoryClient",
            return_value=AsyncMock(),
        ) as mock:
            client = mock.return_value
            yield client

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "raw_bytes, expected_subject, expected_body_substring, expected_subtype",
        [
            (_PLAIN_ONLY, "Plain only test", "plain text body", "plain"),
            (_ALTERNATIVE, "Both parts", "Plain version of the body.", "plain"),
            (_HTML_ONLY, "HTML only", "HTML only body content", "html"),
            (
                _MIXED_WITH_ATTACHMENT,
                "With attachment",
                "Body with a PDF attached.",
                "plain",
            ),
            (
                _RELATED_INLINE_IMAGE,
                "HTML with inline image",
                "Inline image body.",
                "html",
            ),
            (
                _ENCODED_SUBJECT,
                "\u3053\u3093\u306b\u3061\u306f",
                "Body with an encoded subject.",
                "plain",
            ),
            (
                _WINDOWS_1252_BODY,
                "Windows-1252 body",
                "Smart quote: \u201chello\u201d world.",
                "plain",
            ),
        ],
    )
    async def test_message_doc_extracts_body_into_minimal_eml(
        self, raw_bytes, expected_subject, expected_body_substring, expected_subtype
    ):
        raw_b64url = _b64url(raw_bytes)
        message = {
            "id": MESSAGE_ID,
            "raw": raw_b64url,
            "internalDate": CREATION_DATE,
        }

        async with create_gmail_source() as source:
            doc = source._message_doc(message)

        assert doc["_id"] == MESSAGE_ID
        assert doc["_timestamp"] == CREATION_DATE
        assert doc["_attachment"], "Expected non-empty _attachment payload"

        rebuilt = _decode_attachment(doc["_attachment"])

        for noisy in _NOISY_HEADERS:
            assert (
                rebuilt[noisy] is None
            ), f"Noisy header {noisy!r} leaked into the trimmed _attachment"

        assert rebuilt["Subject"] == expected_subject
        assert rebuilt["From"] == "sender@example.com"
        assert rebuilt["To"] == "recipient@example.com"
        assert rebuilt["Date"] is not None

        body = rebuilt.get_body(preferencelist=("plain", "html"))
        assert body is not None
        assert body.get_content_subtype() == expected_subtype
        assert expected_body_substring in body.get_content()

    @pytest.mark.asyncio
    async def test_message_doc_handles_dsn_with_no_textual_body(self):
        raw_b64url = _b64url(_DSN)
        message = {
            "id": MESSAGE_ID,
            "raw": raw_b64url,
            "internalDate": CREATION_DATE,
        }

        async with create_gmail_source() as source:
            doc = source._message_doc(message)

        assert doc["_attachment"]
        rebuilt = _decode_attachment(doc["_attachment"])

        for noisy in _NOISY_HEADERS:
            assert rebuilt[noisy] is None

        assert rebuilt["Subject"] == "Delivery Status Notification (Failure)"
        # No usable body part - the rebuilt message is allowed to have an empty body,
        # but the trimmed payload must remain a valid email with the kept headers.
        body = rebuilt.get_body(preferencelist=("plain", "html"))
        assert body is None or body.get_content().strip() == ""

    @pytest.mark.asyncio
    async def test_message_doc_falls_back_when_extraction_fails(self, monkeypatch):
        monkeypatch.setattr(
            "connectors.sources.gmail.datasource._extract_body_eml",
            lambda _raw: None,
        )

        raw_b64url = _b64url(_PLAIN_ONLY)
        message = {
            "id": MESSAGE_ID,
            "raw": raw_b64url,
            "internalDate": CREATION_DATE,
        }

        async with create_gmail_source() as source:
            doc = source._message_doc(message)

        assert doc["_id"] == MESSAGE_ID
        assert doc["_timestamp"] == CREATION_DATE
        # Falls back to today's behavior: legacy base64url -> base64.
        assert doc["_attachment"] == base64url_to_base64(raw_b64url)

    @pytest.mark.asyncio
    async def test_message_doc_with_full_raw_toggle_passes_through_unchanged(self):
        raw_b64url = _b64url(_PLAIN_ONLY)
        message = {
            "id": MESSAGE_ID,
            "raw": raw_b64url,
            "internalDate": CREATION_DATE,
        }

        async with create_gmail_source(include_full_raw_message=True) as source:
            doc = source._message_doc(message)

        # Legacy path: payload is the raw message with base64url -> base64 conversion.
        assert doc["_attachment"] == base64url_to_base64(raw_b64url)
        # Sanity check: the noisy headers ARE still present when the toggle is on,
        # which is the entire point of providing the toggle.
        rebuilt = _decode_attachment(doc["_attachment"])
        assert rebuilt["Received"] is not None
        assert rebuilt["DKIM-Signature"] is not None

    @pytest.mark.asyncio
    @freeze_time(DATE)
    @pytest.mark.parametrize(
        "raw, expected_timestamp",
        [
            (None, DATE),
            ("", DATE),
        ],
    )
    async def test_message_doc_passes_through_empty_or_none_raw(
        self, raw, expected_timestamp
    ):
        async with create_gmail_source() as source:
            doc = source._message_doc(
                {"id": MESSAGE_ID, "raw": raw, "internalDate": None}
            )

        assert doc["_id"] == MESSAGE_ID
        assert doc["_attachment"] == raw
        assert doc["_timestamp"] == expected_timestamp

    @pytest.mark.asyncio
    @freeze_time(DATE)
    async def test_message_doc_defaults_timestamp_when_missing(self):
        raw_b64url = _b64url(_PLAIN_ONLY)

        async with create_gmail_source() as source:
            doc = source._message_doc(
                {"id": MESSAGE_ID, "raw": raw_b64url, "internalDate": None}
            )

        assert doc["_timestamp"] == DATE

    @pytest.mark.parametrize(
        "raw_b64url",
        [
            "not!valid?base64",
            "!@#$%^&*",
            "###",
        ],
    )
    def test_extract_body_eml_never_raises_on_garbage_input(self, raw_b64url):
        # The helper is best-effort: on garbage input it must either return None
        # (so the caller falls back to the legacy payload) or a valid base64
        # string. It must never raise.
        result = _extract_body_eml(raw_b64url)
        assert result is None or isinstance(result, str)

    def test_extract_body_eml_passes_through_empty_input(self):
        assert _extract_body_eml(None) is None
        assert _extract_body_eml("") == ""

    @pytest.mark.asyncio
    async def test_ping_successful(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with create_gmail_source() as source:
            patch_gmail_client.ping = AsyncMock()
            patch_google_directory_client.ping = AsyncMock()

            try:
                await source.ping()
            except Exception as e:
                msg = "Ping should've been successful"
                raise AssertionError(msg) from e

    @pytest.mark.asyncio
    async def test_ping_gmail_client_fails(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with create_gmail_source() as source:
            patch_gmail_client.ping = AsyncMock(
                side_effect=Exception("Something went wrong")
            )
            patch_google_directory_client.ping = AsyncMock()

            with pytest.raises(Exception):
                await source.ping()

    @pytest.mark.asyncio
    async def test_ping_google_directory_client_fails(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with create_gmail_source() as source:
            patch_gmail_client.ping = AsyncMock()
            patch_google_directory_client.ping = AsyncMock(side_effect=Exception)

            with pytest.raises(Exception):
                await source.ping()

    @pytest.mark.asyncio
    async def test_validate_config_valid(
        self, patch_gmail_client, patch_google_directory_client
    ):
        valid_json = '{"project_id": "dummy123"}'

        async with create_gmail_source() as source:
            source.configuration.get_field(
                "service_account_credentials"
            ).value = valid_json
            source.configuration.get_field("customer_id").value = CUSTOMER_ID
            source.configuration.get_field("subject").value = SUBJECT

            patch_gmail_client.ping = AsyncMock()
            patch_google_directory_client.ping = AsyncMock()

            try:
                await source.validate_config()
            except ConfigurableFieldValueError:
                msg = "Should've been a valid config"
                raise AssertionError(msg) from None

    @pytest.mark.asyncio
    async def test_validate_config_invalid_service_account_credentials(self):
        async with create_gmail_source() as source:
            source.configuration.get_field(
                "service_account_credentials"
            ).value = "invalid json"

            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()

    @pytest.mark.asyncio
    async def test_validate_config_invalid_subject(self):
        async with create_gmail_source() as source:
            source.configuration.get_field("subject").value = "invalid address"

            with pytest.raises(ConfigurableFieldValueError):
                await source.validate_config()

    @pytest.mark.asyncio
    async def test_validate_config_invalid_gmail_auth(
        self, patch_gmail_client, patch_google_directory_client
    ):
        async with create_gmail_source() as source:
            patch_gmail_client.ping = AsyncMock(
                side_effect=AuthError("some auth error")
            )
            patch_google_directory_client.ping = AsyncMock()

            with pytest.raises(ConfigurableFieldValueError) as e:
                await source.validate_config()

            # Make sure this is a GMail auth error
            assert "GMail auth" in str(e.value)

    @pytest.mark.asyncio
    async def test_validate_config_invalid_google_directory_auth(
        self, patch_google_directory_client
    ):
        async with create_gmail_source() as source:
            patch_google_directory_client.ping = AsyncMock(
                side_effect=AuthError("some auth error")
            )

            with pytest.raises(ConfigurableFieldValueError) as e:
                await source.validate_config()

            # Make sure this is a Google Directory auth error
            assert "Google Directory auth" in str(e.value)

    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_disabled(
        self, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        patch_google_directory_client.users = AsyncIterator(users)

        async with create_gmail_source() as source:
            actual_users = []

            async for user in source.get_access_control():
                actual_users.append(user)

            assert len(actual_users) == 0
            patch_google_directory_client.users.assert_not_called()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_access_control_with_dls_enabled(
        self, patch_google_directory_client
    ):
        email = "user@google.com"
        creation_date = iso_utc()
        users = [
            {
                UserFields.EMAIL.value: email,
                UserFields.CREATION_DATE.value: creation_date,
            }
        ]
        patch_google_directory_client.users = AsyncIterator(users)

        async with create_gmail_source(dls_enabled=True) as source:
            actual_users = []

            async for user in source.get_access_control():
                actual_users.append(user)

            actual_user = actual_users[0]

            assert len(actual_users) == len(users)
            assert actual_user["_id"] == email
            assert actual_user["identity"]["email"] == email
            assert actual_user["created_at"] == creation_date

            patch_google_directory_client.users.assert_called_once()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_dls_without_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source() as source:
            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL not in actual_message

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_dls_with_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        users = [{UserFields.EMAIL.value: "user@google.com"}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source() as source:
            actual_messages = []
            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL not in actual_message

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=ANY
            )

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_dls_without_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source(dls_enabled=True) as source:
            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL in actual_message
            assert email in actual_message[ACCESS_CONTROL]

            patch_gmail_client.messages.assert_called_once()

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_dls_with_filtering(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source(dls_enabled=True) as source:
            actual_messages = []
            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"
            assert ACCESS_CONTROL in actual_message
            assert email in actual_message[ACCESS_CONTROL]

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=ANY
            )

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_without_filtering_and_include_spam_and_trash(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source(
            dls_enabled=False, include_spam_and_trash=True
        ) as source:
            actual_messages = []

            async for doc in source.get_docs(filtering=None):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"

            patch_gmail_client.messages.assert_called_once_with(includeSpamTrash=True)

    @freeze_time(TIME)
    @pytest.mark.asyncio
    async def test_get_docs_with_filtering_and_include_spam_and_trash(
        self, patch_gmail_client, patch_google_directory_client
    ):
        email = "user@google.com"
        users = [{UserFields.EMAIL.value: email}]
        message = {
            MessageFields.ID.value: "1",
            MessageFields.FULL_MESSAGE.value: SAMPLE_RAW_BASE64URL,
            MessageFields.CREATION_DATE.value: iso_utc(),
        }
        messages = [message]

        await setup_messages_and_users_apis(
            patch_gmail_client, patch_google_directory_client, messages, users
        )

        async with create_gmail_source(
            dls_enabled=False, include_spam_and_trash=True
        ) as source:
            actual_messages = []

            message_query = "some query"
            filter_ = Filter(
                {"advanced_snippet": {"value": {"messages": [message_query]}}}
            )

            async for doc in source.get_docs(filtering=filter_):
                actual_messages.append(doc)

            actual_message = actual_messages[0][0]

            assert len(actual_messages) == 1
            assert actual_message["_id"] == message[MessageFields.ID.value]
            assert len(actual_message["_attachment"]) > 0
            assert actual_message["_timestamp"] == "2023-01-24T04:07:19+00:00"

            patch_gmail_client.messages.assert_called_once_with(
                query=message_query, includeSpamTrash=True
            )

    @pytest.mark.parametrize(
        "feature_enabled_, rcf_enabled_, dls_enabled_",
        [
            (dls_feature_enabled(False), dls_rcf_enabled(False), dls_enabled(False)),
            (dls_feature_enabled(True), dls_rcf_enabled(False), dls_enabled(False)),
            (dls_feature_enabled(False), dls_rcf_enabled(True), dls_enabled(False)),
            (dls_feature_enabled(None), dls_rcf_enabled(True), dls_enabled(False)),
            (dls_feature_enabled(True), dls_rcf_enabled(True), dls_enabled(True)),
        ],
    )
    @pytest.mark.asyncio
    async def test_dls_enabled(self, feature_enabled_, rcf_enabled_, dls_enabled_):
        async with create_gmail_source(dls_enabled=rcf_enabled_) as source:
            # `dls_enabled` sets both the feature flag and the config value in create_gmail_source
            # -> set dls feature flag after instantiation again
            source.set_features(
                Features({"document_level_security": {"enabled": feature_enabled_}})
            )

            assert source._dls_enabled() == dls_enabled_
