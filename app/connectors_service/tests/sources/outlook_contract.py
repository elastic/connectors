#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Golden Elasticsearch document contract for Outlook-family connectors (#4346 §4)."""

MAIL_DOC_TYPES = frozenset(
    {
        "Inbox Mails",
        "Sent Mails",
        "Junk Mails",
        "Archive Mails",
        "Mail",
    }
)

CALENDAR_DOC_TYPE = "Calendar"
CONTACT_DOC_TYPE = "Contact"
DISTRIBUTION_LIST_DOC_TYPE = "Distribution List"
TASK_DOC_TYPE = "Task"

ATTACHMENT_DOC_TYPES = frozenset(
    {
        "Mail Attachment",
        "Calendar Attachment",
        "Task Attachment",
    }
)

IMPORTANCE_VALUES = frozenset({"Low", "Normal", "High", None})
TASK_STATUS_VALUES = frozenset(
    {
        "NotStarted",
        "InProgress",
        "Completed",
        "WaitingOnOthers",
        "Deferred",
        None,
    }
)

MAIL_REQUIRED_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "title",
        "type",
        "sender",
        "to_recipients",
        "cc_recipients",
        "bcc_recipients",
        "importance",
        "categories",
        "message",
        "_attachment",
    }
)

CALENDAR_REGULAR_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "type",
        "title",
        "meeting_type",
        "organizer",
        "attendees",
        "start_date",
        "end_date",
        "location",
        "content",
    }
)

CALENDAR_BIRTHDAY_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "type",
        "title",
        "meeting_type",
        "organizer",
        "date",
    }
)

CONTACT_REQUIRED_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "type",
        "name",
        "email_addresses",
        "contact_numbers",
        "company_name",
        "birthday",
    }
)

DISTRIBUTION_LIST_REQUIRED_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "type",
        "name",
        "email_addresses",
    }
)

TASK_REQUIRED_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "type",
        "title",
        "owner",
        "start_date",
        "due_date",
        "complete_date",
        "categories",
        "importance",
        "content",
        "status",
    }
)

ATTACHMENT_REQUIRED_KEYS = frozenset(
    {
        "_id",
        "_timestamp",
        "title",
        "type",
        "size",
    }
)


def _is_str_or_none(value):
    return value is None or isinstance(value, str)


def _is_str_list(value):
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _assert_timestamp(value):
    assert _is_str_or_none(value)
    if value is not None:
        assert value.endswith("Z"), value
        assert "T" in value, value


def assert_mail_document(document):
    assert set(document.keys()) >= MAIL_REQUIRED_KEYS
    optional = {"folder_name", "attachments"}
    assert set(document.keys()) <= MAIL_REQUIRED_KEYS | optional
    assert isinstance(document["_id"], str)
    _assert_timestamp(document["_timestamp"])
    assert _is_str_or_none(document["title"])
    assert document["type"] in MAIL_DOC_TYPES
    assert _is_str_or_none(document["sender"])
    assert _is_str_list(document["to_recipients"])
    assert _is_str_list(document["cc_recipients"])
    assert _is_str_list(document["bcc_recipients"])
    assert document["importance"] in IMPORTANCE_VALUES
    assert isinstance(document["categories"], list)
    assert all(isinstance(category, str) for category in document["categories"])
    assert _is_str_or_none(document["message"])
    assert isinstance(document["_attachment"], str)
    if "folder_name" in document:
        assert isinstance(document["folder_name"], str)
    if "attachments" in document:
        assert isinstance(document["attachments"], list)
        for ref in document["attachments"]:
            assert isinstance(ref, dict)
            assert set(ref.keys()) == {"id", "title"}


def assert_calendar_document(document):
    assert document["type"] == CALENDAR_DOC_TYPE
    if "date" in document:
        assert CALENDAR_BIRTHDAY_KEYS <= set(document.keys())
        assert set(document.keys()) <= CALENDAR_BIRTHDAY_KEYS | {"attachments"}
        _assert_timestamp(document["_timestamp"])
        assert _is_str_or_none(document["date"])
    else:
        allowed = CALENDAR_REGULAR_KEYS | {"attachments"}
        assert set(document.keys()) <= allowed
        assert set(document.keys()) >= CALENDAR_REGULAR_KEYS
        _assert_timestamp(document["_timestamp"])
        assert _is_str_list(document["attendees"])
        _assert_timestamp(document["start_date"])
        _assert_timestamp(document["end_date"])


def assert_contact_document(document):
    assert set(document.keys()) >= CONTACT_REQUIRED_KEYS
    assert document["type"] == CONTACT_DOC_TYPE
    _assert_timestamp(document["_timestamp"])
    assert _is_str_list(document["email_addresses"])
    assert _is_str_list(document["contact_numbers"])


def assert_distribution_list_document(document):
    assert set(document.keys()) == DISTRIBUTION_LIST_REQUIRED_KEYS
    assert document["type"] == DISTRIBUTION_LIST_DOC_TYPE
    _assert_timestamp(document["_timestamp"])
    assert _is_str_list(document["email_addresses"])


def assert_task_document(document):
    assert set(document.keys()) >= TASK_REQUIRED_KEYS
    optional = {"attachments"}
    assert set(document.keys()) <= TASK_REQUIRED_KEYS | optional
    assert document["type"] == TASK_DOC_TYPE
    _assert_timestamp(document["_timestamp"])
    assert document["importance"] in IMPORTANCE_VALUES
    assert document["status"] in TASK_STATUS_VALUES


def assert_attachment_document(document):
    assert set(document.keys()) >= ATTACHMENT_REQUIRED_KEYS
    optional = {"parent_id"}
    assert set(document.keys()) <= ATTACHMENT_REQUIRED_KEYS | optional
    assert document["type"] in ATTACHMENT_DOC_TYPES
    _assert_timestamp(document["_timestamp"])
    assert document["size"] is None or isinstance(document["size"], int)


def assert_conforms(document):
    doc_type = document.get("type")
    if doc_type in MAIL_DOC_TYPES:
        assert_mail_document(document)
    elif doc_type == CALENDAR_DOC_TYPE:
        assert_calendar_document(document)
    elif doc_type == CONTACT_DOC_TYPE:
        assert_contact_document(document)
    elif doc_type == DISTRIBUTION_LIST_DOC_TYPE:
        assert_distribution_list_document(document)
    elif doc_type == TASK_DOC_TYPE:
        assert_task_document(document)
    elif doc_type in ATTACHMENT_DOC_TYPES:
        assert_attachment_document(document)
    else:
        msg = f"Unknown Outlook document type: {doc_type!r}"
        raise AssertionError(msg)
