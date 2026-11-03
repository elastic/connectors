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


def _set_header(eml, name, value, logger=None):
    """Set a header; one bad value (CR/LF is folded to a space) must not lose the whole EML."""
    if not isinstance(value, str) or not value:
        return
    value = " ".join(value.splitlines()).strip()
    if not value:
        return
    try:
        eml[name] = value
    except Exception:
        if logger:
            logger.warning(f"Dropping unusable {name} header from Graph message.")


def build_minimal_eml_from_graph_message(message, logger=None):
    """Build a header-light .eml from Graph message JSON."""
    eml = EmailMessage(policy=_DEFAULT_POLICY)

    _set_header(eml, "Subject", message.get("subject"), logger)
    # `from` is the author; `sender` only differs for delegated sends and is the
    # fallback when `from` is absent.
    _set_header(
        eml,
        "From",
        _smtp_address(message.get("from")) or _smtp_address(message.get("sender")),
        logger,
    )
    _set_header(eml, "Reply-To", _reply_to_header(message.get("replyTo")), logger)
    for header, key in (
        ("To", "toRecipients"),
        ("Cc", "ccRecipients"),
        ("Bcc", "bccRecipients"),
    ):
        _set_header(eml, header, _recipient_header(message.get(key)), logger)

    sent = message.get("sentDateTime")
    if isinstance(sent, str) and sent:
        try:
            iso = graph_datetime_to_iso_utc(sent)
            parsed = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            _set_header(eml, "Date", format_datetime(parsed), logger)
        except ValueError:
            if logger:
                logger.warning("Dropping unparsable sentDateTime from Graph message.")

    _set_header(eml, "Message-ID", message.get("internetMessageId"), logger)

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
    """Return the base64 ``_attachment`` payload for a Graph message.

    The raw MIME is used when requested and available; otherwise an .eml is rebuilt
    from the JSON, and as a last resort an empty one.
    """
    if include_full_raw_message and raw_mime_bytes:
        return base64.b64encode(raw_mime_bytes).decode("ascii")
    try:
        eml = build_minimal_eml_from_graph_message(message, logger)
    except Exception:
        logger.warning(
            f"Failed to build minimal EML for {message.get('id', 'unknown')}; using empty body fallback.",
            exc_info=True,
        )
        eml = _empty_minimal_eml_bytes()
    return base64.b64encode(eml).decode("ascii")
