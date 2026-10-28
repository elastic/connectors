#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

from connectors.sources.outlook_cloud.mail_eml import _smtp_address
from connectors.sources.outlook_cloud.utils import (
    graph_datetime_to_iso_utc,
    graph_importance_to_legacy,
)
from connectors.utils import html_to_text


class OutlookCloudDocFormatter:
    """Format Microsoft Graph message JSON to Elasticsearch documents."""

    @staticmethod
    def _document_sender(message):
        sender = _smtp_address(message.get("sender"))
        if sender:
            return sender
        return _smtp_address(message.get("from"))

    def mail_doc_formatter(self, message, mail_type):
        document = {
            "_id": message["id"],
            "_timestamp": graph_datetime_to_iso_utc(message.get("lastModifiedDateTime")),
            "title": message.get("subject"),
            "type": mail_type["constant"],
            "sender": self._document_sender(message),
            "to_recipients": _recipient_list(message.get("toRecipients")),
            "cc_recipients": _recipient_list(message.get("ccRecipients")),
            "bcc_recipients": _recipient_list(message.get("bccRecipients")),
            "importance": graph_importance_to_legacy(message.get("importance")),
            "categories": list(message.get("categories") or []),
            "message": _message_text(message.get("body")),
        }
        folder_name = mail_type.get("folder_name")
        if folder_name is not None:
            document["folder_name"] = folder_name
        return document


def _recipient_list(recipients):
    if not isinstance(recipients, list):
        return []
    return [
        address
        for address in (_smtp_address(recipient) for recipient in recipients)
        if address
    ]


def _message_text(body):
    if not isinstance(body, dict):
        return None
    content = body.get("content")
    if not isinstance(content, str):
        return None
    if body.get("contentType") == "text":
        return content
    return html_to_text(html=content)
