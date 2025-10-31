#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#


RETRIES = 3
RETRY_INTERVAL = 2

QUEUE_MEM_SIZE = 5 * 1024 * 1024  # Size in Megabytes

OUTLOOK_SERVER = "outlook_server"
OUTLOOK_CLOUD = "outlook_cloud"
API_SCOPE = "https://graph.microsoft.com/.default"
EWS_ENDPOINT = "https://outlook.office365.com/EWS/Exchange.asmx"
TOP = 999

DEFAULT_TIMEZONE = "UTC"

INBOX_MAIL_OBJECT = "Inbox Mails"
SENT_MAIL_OBJECT = "Sent Mails"
JUNK_MAIL_OBJECT = "Junk Mails"
ARCHIVE_MAIL_OBJECT = "Archive Mails"
MAIL_ATTACHMENT = "Mail Attachment"
TASK_ATTACHMENT = "Task Attachment"
CALENDAR_ATTACHMENT = "Calendar Attachment"

SEARCH_FILTER_FOR_NORMAL_USERS = (
    "(&(objectCategory=person)(objectClass=user)(givenName=*))"
)
SEARCH_FILTER_FOR_ADMIN = "(&(objectClass=person)(|(cn=*admin*)(cn=*normal*)))"

MAIL_TYPES = [
    {
        "folder": "inbox",
        "constant": INBOX_MAIL_OBJECT,
    },
    {
        "folder": "sent",
        "constant": SENT_MAIL_OBJECT,
    },
    {
        "folder": "junk",
        "constant": JUNK_MAIL_OBJECT,
    },
    {
        "folder": "archive",
        "constant": ARCHIVE_MAIL_OBJECT,
    },
]

MAIL_FIELDS = [
    "sender",
    "to_recipients",
    "cc_recipients",
    "bcc_recipients",
    "last_modified_time",
    "subject",
    "importance",
    "categories",
    "body",
    "has_attachments",
    "attachments",
]
CONTACT_FIELDS = [
    "email_addresses",
    "phone_numbers",
    "last_modified_time",
    "display_name",
    "company_name",
    "birthday",
]
TASK_FIELDS = [
    "last_modified_time",
    "due_date",
    "complete_date",
    "subject",
    "status",
    "owner",
    "start_date",
    "text_body",
    "companies",
    "categories",
    "importance",
    "has_attachments",
    "attachments",
]
CALENDAR_FIELDS = [
    "required_attendees",
    "type",
    "recurrence",
    "last_modified_time",
    "subject",
    "start",
    "end",
    "location",
    "organizer",
    "body",
    "has_attachments",
    "attachments",
]

END_SIGNAL = "FINISHED"
CERT_FILE = "outlook_cert.cer"
