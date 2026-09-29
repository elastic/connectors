#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from typing import cast

# Headers kept when trimming; everything else is dropped.
KEPT_HEADERS = (
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


def trim_rfc822_bytes_to_base64(raw_bytes):
    """Parse RFC 822 bytes and rebuild a minimal .eml with only KEPT_HEADERS and one
    body part (``text/plain`` preferred, ``text/html`` fallback).

    Returns standard base64 ready for ``_attachment``, or ``None`` on failure.
    """
    try:
        original = cast(
            EmailMessage,
            BytesParser(_class=EmailMessage, policy=_DEFAULT_POLICY).parsebytes(
                raw_bytes
            ),
        )

        rebuilt = EmailMessage(policy=_DEFAULT_POLICY)
        for header in KEPT_HEADERS:
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
            rebuilt.set_content("", subtype="plain", charset="utf-8")

        return base64.b64encode(rebuilt.as_bytes()).decode("ascii")
    except Exception:
        return None


def extract_body_eml(raw_base64url):
    """Trim a Gmail base64url RFC 822 message. Returns standard base64, ``None`` on
    parse failure, or the input unchanged when empty.
    """
    if not raw_base64url:
        return raw_base64url

    try:
        raw_bytes = base64.urlsafe_b64decode(raw_base64url + "===")
    except Exception:
        return None

    return trim_rfc822_bytes_to_base64(raw_bytes)
