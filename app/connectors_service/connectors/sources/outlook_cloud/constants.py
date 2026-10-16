#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"

# Graph returns stable ids that do not change when an item moves between folders.
GRAPH_REQUEST_HEADERS = {"Prefer": 'IdType="ImmutableId"'}

USER_FIELDS = "id,displayName,mail,jobTitle"
USERS_PAGE_SIZE = 999

REQUEST_TIMEOUT = 300  # seconds
RETRIES = 5
RETRYABLE_STATUSES = frozenset({429, 503, 504})
DEFAULT_RETRY_SECONDS = 30  # used when Graph sends no usable Retry-After header

# How many mailboxes are handled at the same time. Not exposed in the configuration.
MAX_CONCURRENT_MAILBOXES = 10

# Result of probing a user's mailbox.
MAILBOX_ACCESSIBLE = "accessible"
MAILBOX_OUT_OF_SCOPE = "out_of_scope"  # Graph answered 403
MAILBOX_MISSING = "missing"  # Graph answered 404, or the user has no mail address
