#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from datetime import date, datetime
from email import policy
from email.message import EmailMessage
from email.utils import format_datetime

from connectors.sources.shared.email_trim import trim_rfc822_bytes_to_base64
from connectors.utils import html_to_text

_DEFAULT_POLICY = policy.default


def _recipient_addresses(recipients):
    if not isinstance(recipients, (list, tuple)):
        return None
    addresses = [
        recipient.email_address
        for recipient in recipients
        if recipient
        and isinstance(getattr(recipient, "email_address", None), str)
        and recipient.email_address
    ]
    return ", ".join(addresses) if addresses else None


def _reply_to_addresses(reply_to):
    if isinstance(reply_to, (list, tuple)):
        return _recipient_addresses(reply_to)
    email_address = getattr(reply_to, "email_address", None)
    if isinstance(email_address, str) and email_address:
        return email_address
    return None


def build_minimal_eml_from_mail(mail):
    """Build a header-light .eml from EWS message fields when MIME is unavailable."""
    message = EmailMessage(policy=_DEFAULT_POLICY)

    subject = getattr(mail, "subject", None)
    if isinstance(subject, str) and subject:
        message["Subject"] = subject

    sender = getattr(mail, "sender", None)
    sender_address = getattr(sender, "email_address", None) if sender else None
    if isinstance(sender_address, str) and sender_address:
        message["From"] = sender_address

    reply_to = _reply_to_addresses(getattr(mail, "reply_to", None))
    if reply_to:
        message["Reply-To"] = reply_to

    to_addresses = _recipient_addresses(getattr(mail, "to_recipients", None))
    if to_addresses:
        message["To"] = to_addresses

    cc_addresses = _recipient_addresses(getattr(mail, "cc_recipients", None))
    if cc_addresses:
        message["Cc"] = cc_addresses

    bcc_addresses = _recipient_addresses(getattr(mail, "bcc_recipients", None))
    if bcc_addresses:
        message["Bcc"] = bcc_addresses

    datetime_received = getattr(mail, "datetime_received", None)
    if isinstance(datetime_received, (datetime, date)):
        message["Date"] = format_datetime(datetime_received)

    message_id = getattr(mail, "message_id", None)
    if isinstance(message_id, str) and message_id:
        message["Message-ID"] = message_id

    text_body = getattr(mail, "text_body", None)
    if isinstance(text_body, str) and text_body:
        message.set_content(text_body, subtype="plain", charset="utf-8")
    else:
        body = getattr(mail, "body", None)
        if isinstance(body, str) and body:
            message.set_content(
                html_to_text(html=body) or "",
                subtype="plain",
                charset="utf-8",
            )
        else:
            message.set_content("", subtype="plain", charset="utf-8")

    return message.as_bytes()


def mail_attachment_base64(mail, include_full_raw_message, logger):
    """Return base64 ``_attachment`` payload for an Outlook mail item."""
    mime_content = getattr(mail, "mime_content", None)
    if mime_content is not None and not isinstance(mime_content, (bytes, bytearray)):
        mime_content = None
    mail_id = getattr(mail, "id", "unknown")

    if include_full_raw_message:
        if mime_content:
            return base64.b64encode(mime_content).decode("ascii")
        return base64.b64encode(build_minimal_eml_from_mail(mail)).decode("ascii")

    if mime_content:
        attachment = trim_rfc822_bytes_to_base64(mime_content)
        if attachment is not None:
            return attachment

        logger.warning(
            "Body extraction failed for %s; falling back to raw MIME payload.",
            mail_id,
        )
        return base64.b64encode(mime_content).decode("ascii")

    return base64.b64encode(build_minimal_eml_from_mail(mail)).decode("ascii")
