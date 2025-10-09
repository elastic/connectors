#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
from datetime import datetime, timedelta

from connectors_sdk.logger import logger

from connectors.utils import CacheWithTimeout

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


class TokenError(Exception):
    pass


class NotFound(Exception):
    pass


class AccessToken:
    def __init__(self, configuration, http_session):
        global refresh_token
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self._http_session = http_session
        if refresh_token is None:
            refresh_token = configuration["refresh_token"]
        self._token_cache = CacheWithTimeout()
        self.is_enterprise = configuration["is_enterprise"]
        self.enterprise_id = configuration["enterprise_id"]

    async def get(self):
        if cached_value := self._token_cache.get_value():
            return cached_value
        logger.debug("No token cache found; fetching new token")
        await self._set_access_token()
        return self.access_token

    async def _set_access_token(self):
        logger.debug("Generating an access token")
        try:
            if self.is_enterprise == BOX_FREE:
                global refresh_token
                data = {
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
                async with self._http_session.post(
                    url=ENDPOINTS["TOKEN"],
                    data=data,
                ) as response:
                    tokens = await response.json()
                    self.access_token = tokens.get("access_token")
                    refresh_token = tokens.get("refresh_token")
                    self.expired_at = datetime.utcnow() + timedelta(
                        seconds=int(tokens.get("expires_in", 3599))
                    )
                    self._token_cache.set_value(
                        value=self.access_token, expiration_date=self.expired_at
                    )
            else:
                data = {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                    "box_subject_type": "enterprise",
                    "box_subject_id": self.enterprise_id,
                }
                async with self._http_session.post(
                    url=ENDPOINTS["TOKEN"],
                    data=data,
                ) as response:
                    tokens = await response.json()
                    self.access_token = tokens.get("access_token")
                    self.expired_at = datetime.utcnow() + timedelta(
                        seconds=int(tokens.get("expires_in", 3599))
                    )
                    self._token_cache.set_value(
                        value=self.access_token, expiration_date=self.expired_at
                    )
        except Exception as exception:
            msg = f"Error while generating access token. Please verify that provided configurations are correct. Exception {exception}."
            raise TokenError(msg) from exception
