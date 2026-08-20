#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import json
from datetime import datetime

from connectors.access_control import prefix_identity
from connectors.sources.sharepoint.sharepoint_online.constants import (
    EXCLUDED_SHAREPOINT_PATH_SEGMENTS,
    TIMESTAMP_FORMAT,
)


class SyncCursorEmpty(Exception):
    """Exception class to notify that incremental sync can't run because sync_cursor is empty.
    See: https://learn.microsoft.com/en-us/graph/delta-query-overview
    """

    pass


class DeltaLinkExpired(Exception):
    """Raised when Microsoft Graph returns 410 Gone for a delta link."""

    pass


def _sharepoint_list_item_id(graph_page):
    sharepoint_ids = (
        graph_page.get("sharePointIds") or graph_page.get("sharepointIds") or {}
    )
    return sharepoint_ids.get("listItemId") or graph_page.get("id")


def _graph_page_body_content(graph_page):
    description = graph_page.get("description") or ""
    canvas_layout = graph_page.get("canvasLayout")

    if canvas_layout is None:
        return description

    if isinstance(canvas_layout, str):
        return canvas_layout or description

    try:
        return json.dumps(canvas_layout)
    except (TypeError, ValueError):
        return description


def graph_site_page_to_document(graph_page):
    """Map a Microsoft Graph sitePage resource to the connector document shape."""
    page_id = graph_page.get("id")
    list_item_id = _sharepoint_list_item_id(graph_page)
    body_content = _graph_page_body_content(graph_page)

    return {
        "Id": list_item_id,
        "graph_page_id": page_id,
        "Title": graph_page.get("title") or graph_page.get("name"),
        "webUrl": graph_page.get("webUrl"),
        "LayoutWebpartsContent": graph_page.get("layoutWebpartsContent"),
        "CanvasContent1": body_content,
        "WikiField": body_content,
        "Description": graph_page.get("description") or "",
        "Created": graph_page.get("createdDateTime"),
        "Modified": graph_page.get("lastModifiedDateTime"),
        "AuthorId": graph_page.get("authorId"),
        "EditorId": graph_page.get("editorId"),
        "odata.id": list_item_id,
        "OData__UIVersionString": graph_page.get("uiVersionString"),
    }


def _prefix_group(group):
    return prefix_identity("group", group)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_email(email):
    return prefix_identity("email", email)


def _get_login_name(raw_login_name):
    if raw_login_name and (
        raw_login_name.startswith("i:0#.f|membership|")
        or raw_login_name.startswith("c:0o.c|federateddirectoryclaimprovider|")
        or raw_login_name.startswith("c:0t.c|tenant|")
    ):
        parts = raw_login_name.split("|")

        if len(parts) > 2:
            return parts[2]

    return None


def _parse_created_date_time(created_date_time):
    if created_date_time is None:
        return None
    return datetime.strptime(created_date_time, TIMESTAMP_FORMAT)


def _is_excluded_sharepoint_url(url: str) -> bool:
    try:
        return any(
            segment in url.lower() for segment in EXCLUDED_SHAREPOINT_PATH_SEGMENTS
        )
    except Exception:
        return False
