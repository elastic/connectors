#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from datetime import datetime, timezone
from email import policy
from email.message import EmailMessage
from email.utils import format_datetime

from connectors.sources.outlook_cloud.utils import graph_datetime_to_iso_utc
from connectors.utils import html_to_text

_DEFAULT_POLICY = policy.default


def _smtp_address(recipient):
    if not isinstance(recipient, dict):
        return None
    address = recipient.get("emailAddress") or {}
    email = address.get("address")
    return email if isinstance(email, str) and email else None


def _recipient_header(recipients):
    if not isinstance(recipients, list):
        return None
    addresses = [_smtp_address(recipient) for recipient in recipients]
    addresses = [address for address in addresses if address]
    return ", ".join(addresses) if addresses else None


def _reply_to_header(reply_to):
    if isinstance(reply_to, list):
        return _recipient_header(reply_to)
    return _smtp_address(reply_to)


def _body_plain_text(body):
    if not isinstance(body, dict):
        return ""
    content = body.get("content")
    if not isinstance(content, str):
        return ""
    if body.get("contentType") == "text":
        return content
    return html_to_text(html=content) or ""


def build_minimal_eml_from_graph_message(message):
    """Build a header-light .eml from Graph message JSON."""
    eml = EmailMessage(policy=_DEFAULT_POLICY)

    subject = message.get("subject")
    if isinstance(subject, str) and subject:
        eml["Subject"] = subject

    from_address = _smtp_address(message.get("from"))
    if from_address:
        eml["From"] = from_address

    reply_to = _reply_to_header(message.get("replyTo"))
    if reply_to:
        eml["Reply-To"] = reply_to

    for header, key in (
        ("To", "toRecipients"),
        ("Cc", "ccRecipients"),
        ("Bcc", "bccRecipients"),
    ):
        addresses = _recipient_header(message.get(key))
        if addresses:
            eml[header] = addresses

    sent = message.get("sentDateTime")
    if isinstance(sent, str) and sent:
        iso = graph_datetime_to_iso_utc(sent)
        if iso:
            parsed = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            eml["Date"] = format_datetime(parsed)

    message_id = message.get("internetMessageId")
    if isinstance(message_id, str) and message_id:
        eml["Message-ID"] = message_id

    eml.set_content(
        _body_plain_text(message.get("body")),
        subtype="plain",
        charset="utf-8",
    )
    return eml.as_bytes()


def _empty_minimal_eml_bytes():
    message = EmailMessage(policy=_DEFAULT_POLICY)
    message.set_content("", subtype="plain", charset="utf-8")
    return message.as_bytes()


def mail_attachment_base64(message, include_full_raw_message, raw_mime_bytes, logger):
    """Return base64 ``_attachment`` payload for a Graph message."""
    message_id = message.get("id", "unknown")
    if include_full_raw_message:
        if raw_mime_bytes:
            return base64.b64encode(raw_mime_bytes).decode("ascii")
        try:
            return base64.b64encode(build_minimal_eml_from_graph_message(message)).decode(
                "ascii"
            )
        except Exception:
            logger.warning(
                "Failed to build minimal EML for %s; using empty body fallback.",
                message_id,
                exc_info=True,
            )
            return base64.b64encode(_empty_minimal_eml_bytes()).decode("ascii")

    try:
        return base64.b64encode(build_minimal_eml_from_graph_message(message)).decode(
            "ascii"
        )
    except Exception:
        logger.warning(
            "Failed to build minimal EML for %s; using empty body fallback.",
            message_id,
            exc_info=True,
        )
        return base64.b64encode(_empty_minimal_eml_bytes()).decode("ascii")
