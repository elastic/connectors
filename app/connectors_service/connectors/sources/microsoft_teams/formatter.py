#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.sources.microsoft_teams.client import TeamsObjectType


class MicrosoftTeamsFormatter:
    """Format Microsoft Graph objects into Elasticsearch documents."""

    def __init__(self, schema):
        self.schema = schema

    def map_document_with_schema(self, document, item, document_type):
        """Prepare key mappings for documents.

        Args:
            document (dict): Document being built.
            item (dict): Object returned by Microsoft Graph.
            document_type (callable): Schema method returning the field mapping.
        """
        for elasticsearch_field, graph_field in document_type().items():
            document[elasticsearch_field] = item.get(graph_field)

    def format_doc(self, item, document_type, document):
        result = dict(document)
        self.map_document_with_schema(
            document=result, item=item, document_type=document_type
        )
        return result

    def format_user(self, user_id, name, email, upn=None):
        """Build a User document keyed by Entra user id.

        Emitted for every tenant directory user from Graph ``GET /users``.
        ``name`` / ``email`` / ``upn`` come from ``displayName``, ``mail``, and
        ``userPrincipalName``.
        """
        return {
            "_id": user_id,
            "type": TeamsObjectType.USER.value,
            "name": name or "",
            "email": email or "",
            "upn": upn or "",
        }

    def format_file(self, drive_item, parents=None):
        """Build a File document from a Graph driveItem.

        ``parents`` may include sparse ``channel_id``/``channel_title`` and/or
        ``chat_id``/``chat_title`` (only keys that are known).
        """
        document = {"type": TeamsObjectType.FILE.value}
        self.map_document_with_schema(
            document=document, item=drive_item, document_type=self.schema.file
        )
        for key, value in (parents or {}).items():
            if value:
                document[key] = value
        return document

    def format_channel_message(
        self,
        item,
        channel_id,
        channel_title,
        message_content,
        subject="",
        attachments=None,
    ):
        sender_name, sender_id = self._sender_fields(item)
        document = {
            "type": TeamsObjectType.CHANNEL_MESSAGE.value,
            "sender_name": sender_name,
            "sender_id": sender_id,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "subject": subject,
            "message": message_content,
            "reply_to_id": item.get("replyToId") or "",
            "attachments": attachments or [],
        }
        self.map_document_with_schema(
            document=document, item=item, document_type=self.schema.channel_message
        )
        return document

    def format_chat_message(
        self,
        chat,
        message,
        message_content,
        members,
        subject="",
        attachments=None,
    ):
        sender_name, sender_id = self._sender_fields(message)
        document = {
            "type": TeamsObjectType.CHAT_MESSAGE.value,
            "chat_id": chat.get("id"),
            "chat_title": chat.get("topic") or members,
            "sender_name": sender_name,
            "sender_id": sender_id,
            "subject": subject,
            "message": message_content,
            "attachments": attachments or [],
        }
        self.map_document_with_schema(
            document=document, item=message, document_type=self.schema.chat_message
        )
        if not document.get("url"):
            document["url"] = chat.get("webUrl")
        return document

    def _sender_fields(self, message):
        user = (message.get("from") or {}).get("user") or {}
        return (user.get("displayName") or "").strip(), user.get("id") or ""
