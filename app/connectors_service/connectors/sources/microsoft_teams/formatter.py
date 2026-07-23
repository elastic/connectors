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

    def format_team_member(self, member, team_id, team_name):
        document = {
            "type": TeamsObjectType.TEAM_MEMBER.value,
            "team_name": team_name,
            "user_id": member.get("userId"),
            "roles": ", ".join(member.get("roles", []) or []),
        }
        self.map_document_with_schema(
            document=document, item=member, document_type=self.schema.team_member
        )
        # The membership id is not globally unique; prefix it with the team id.
        document["_id"] = f"{team_id}-{member.get('id')}"
        return document

    def format_channel_message(self, item, channel_name, message_content):
        document = {
            "type": TeamsObjectType.CHANNEL_MESSAGE.value,
            "sender": item["from"]["user"].get("displayName")
            if item.get("from") and item["from"].get("user")
            else "",
            "channel": channel_name,
            "message": message_content,
            "attached_documents": self.format_attachment_names(
                attachments=item.get("attachments")
            ),
        }
        self.map_document_with_schema(
            document=document, item=item, document_type=self.schema.channel_message
        )
        return document

    def format_chat_message(self, chat, message, message_content, members):
        augmented = dict(message)
        augmented.update(
            {
                "title": chat.get("topic") or members,
                "webUrl": chat.get("webUrl"),
                "chatType": chat.get("chatType"),
                "sender": message["from"]["user"].get("displayName")
                if message.get("from") and message["from"].get("user")
                else "",
                "message": message_content,
            }
        )
        document = {
            "type": TeamsObjectType.CHAT_MESSAGE.value,
            "attached_documents": self.format_attachment_names(
                attachments=message.get("attachments")
            ),
        }
        self.map_document_with_schema(
            document=document, item=augmented, document_type=self.schema.chat_message
        )
        return document

    def format_attachment_names(self, attachments):
        if not attachments:
            return ""

        return ",".join(
            attachment.get("name")
            for attachment in attachments
            if attachment.get("name", "")
        )
