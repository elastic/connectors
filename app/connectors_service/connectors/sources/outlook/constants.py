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

DEPRECATION_WARNINGS = {
    OUTLOOK_CLOUD: (
        "The Outlook connector is deprecated and receives no new features. Its "
        "Outlook Cloud mode uses Exchange Web Services (EWS) with the "
        "full_access_as_app permission, which Microsoft is retiring in Exchange "
        "Online. Until then, syncs only work if a tenant admin sets EWSEnabled to "
        "True and lists this connector's application ID in EWSAllowedAppIDs. EWS "
        "is permanently turned off on April 1, 2027. Migrate to the Outlook Cloud "
        "connector (outlook_cloud service type), which uses Microsoft Graph."
    ),
    OUTLOOK_SERVER: (
        "The Outlook connector is deprecated and receives no new features. For "
        "on-premises Exchange, migrate to the Exchange Server connector "
        "(exchange_server service type), which syncs the same content."
    ),
}
API_SCOPE = "https://graph.microsoft.com/.default"
EWS_ENDPOINT = "https://outlook.office365.com/EWS/Exchange.asmx"
TOP = 999

INBOX_MAIL_OBJECT = "Inbox Mails"
SENT_MAIL_OBJECT = "Sent Mails"
JUNK_MAIL_OBJECT = "Junk Mails"
ARCHIVE_MAIL_OBJECT = "Archive Mails"
MAIL_OBJECT = "Mail"
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
    "reply_to",
    "last_modified_time",
    "subject",
    "importance",
    "categories",
    "body",
    "text_body",
    "mime_content",
    "message_id",
    "datetime_received",
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
DISTRIBUTION_LIST_FIELDS = [
    "last_modified_time",
    "display_name",
    "members",
]
# Contacts folder holds both item types, so query the union of their fields.
CONTACT_FOLDER_FIELDS = list(dict.fromkeys(CONTACT_FIELDS + DISTRIBUTION_LIST_FIELDS))
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
