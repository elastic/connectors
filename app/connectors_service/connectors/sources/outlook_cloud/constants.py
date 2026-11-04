#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
GRAPH_HOST_URL = (
    "https://graph.microsoft.com/"  # the only place the access token may be sent
)

# Graph returns stable ids that do not change when an item moves between folders.
# Stable ids, and event times in UTC.
GRAPH_REQUEST_HEADERS = {"Prefer": 'IdType="ImmutableId", outlook.timezone="UTC"'}

USER_FIELDS = "id,displayName,mail,jobTitle"
USERS_PAGE_SIZE = 999

MESSAGE_FIELDS = (
    "id,subject,from,sender,replyTo,toRecipients,ccRecipients,bccRecipients,"
    "sentDateTime,lastModifiedDateTime,internetMessageId,importance,categories,body"
)
MESSAGES_PAGE_SIZE = 100
FOLDER_FIELDS = "id,displayName,childFolderCount"

REQUEST_TIMEOUT = 300  # seconds
RETRIES = 5
RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})
DEFAULT_RETRY_SECONDS = 30  # used when Graph sends no usable Retry-After header
NETWORK_RETRY_SECONDS = (
    2  # first wait after a dropped connection or timeout, doubled each time
)

# How many mailboxes are handled at the same time. Not exposed in the configuration.
MAX_CONCURRENT_MAILBOXES = 10

# Result of probing a user's mailbox.
MAILBOX_ACCESSIBLE = "accessible"
MAILBOX_OUT_OF_SCOPE = "out_of_scope"  # Graph answered 403
MAILBOX_MISSING = "missing"  # Graph answered 404, or the user has no mail address

INBOX_MAIL_OBJECT = "Inbox Mails"
SENT_MAIL_OBJECT = "Sent Mails"
JUNK_MAIL_OBJECT = "Junk Mails"
ARCHIVE_MAIL_OBJECT = "Archive Mails"
MAIL_OBJECT = "Mail"

DEFAULT_MAIL_FOLDERS = (
    {"well_known": "inbox", "constant": INBOX_MAIL_OBJECT},
    {"well_known": "sentitems", "constant": SENT_MAIL_OBJECT},
    {"well_known": "junkemail", "constant": JUNK_MAIL_OBJECT},
    {"well_known": "archive", "constant": ARCHIVE_MAIL_OBJECT},
)

# Well-known folder names excluded from sync_all_mail_folders (design #4346 §2).
EXCLUDED_WELL_KNOWN_FOLDERS = frozenset(
    {
        "clutter",
        "conflicts",
        "conversationhistory",
        "deleteditems",
        "drafts",
        "localfailures",
        "outbox",
        "recoverableitemsdeletions",
        "scheduled",
        "searchfolders",
        "serverfailures",
        "syncissues",
    }
)

MAIL_SEARCH_FOLDER_TYPE_NAME = "mailSearchFolder"

CALENDAR_OBJECT = "Calendar"
# Legacy detects birthdays by this name.
BIRTHDAYS_CALENDAR_NAME = "Birthdays"
CALENDAR_FIELDS = "id,name,owner,isDefaultCalendar"
CALENDARS_PAGE_SIZE = 100
EVENT_FIELDS = (
    "id,subject,type,recurrence,organizer,attendees,start,end,location,body,"
    "lastModifiedDateTime"
)
EVENTS_PAGE_SIZE = 100
