#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os

FINISHED = "FINISHED"

ENDPOINTS = {
    "TOKEN": "/oauth2/token",
    "PING": "/2.0/users/me",
    "FOLDER": "/2.0/folders/{folder_id}/items",
    "CONTENT": "/2.0/files/{file_id}/content",
    "USERS": "/2.0/users",
}
RETRIES = 3
RETRY_INTERVAL = 2
CHUNK_SIZE = 1024
FETCH_LIMIT = 1000
QUEUE_MEM_SIZE = 5 * 1024 * 1024  # ~ 5 MB
MAX_CONCURRENCY = 2000
MAX_CONCURRENT_DOWNLOADS = 15
FIELDS = "name,modified_at,size,type,sequence_id,etag,created_at,modified_at,content_created_at,content_modified_at,description,created_by,modified_by,owned_by,parent,item_status"
FILE = "file"
BOX_FREE = "box_free"
BOX_ENTERPRISE = "box_enterprise"

refresh_token = None

if "BOX_BASE_URL" in os.environ:
    BASE_URL = os.environ.get("BOX_BASE_URL")
else:
    BASE_URL = "https://api.box.com"
