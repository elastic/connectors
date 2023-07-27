#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Salesforce source module responsible to fetch documents from Salesforce."""
import aiohttp

from connectors.source import BaseDataSource
from connectors.utils import retryable

RETRIES = 3
RETRY_INTERVAL = 1

BASE_URL = "https://{domain}.my.salesforce.com"
TOKEN_ENDPOINT = "/services/oauth2/token"


class SalesforceClient:
    def __init__(self, configuration):
        self.session = None
        self.token = None
        self.token_issued_at = None

        self.base_url = BASE_URL.replace("{domain}", configuration["domain"])
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self.token_payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        if self.session is not None:
            return self.session

        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )
        return self.session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
    )
    async def get_token(self):
        resp = await self._api_request(
            f"{self.base_url}{TOKEN_ENDPOINT}", "post", data=self.token_payload
        )
        resp_json = await resp.json()
        self.token = resp_json["access_token"]
        self.token_issued_at = resp_json["issued_at"]

    async def ping(self):
        await self._api_request(self.base_url, "head")

    async def close_session(self):
        if self.session is not None:
            await self.session.close()

    def _auth_headers(self):
        return {"authorization": f"Bearer {self.token}"}

    async def _api_request(self, url, method, data=None):
        return await getattr(self._get_session(), method)(url=url, data=data)


class SalesforceDataSource(BaseDataSource):
    """Salesforce"""

    name = "Salesforce"
    service_type = "salesforce"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)
        self.salesforce_client = SalesforceClient(configuration=configuration)

    def _set_internal_logger(self):
        self.salesforce_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        return {
            "client_id": {
                "type": "str",
                "value": "",
            },
            "client_secret": {
                "type": "str",
                "value": "",
            },
            "domain": {
                "type": "str",
                "value": "",
            },
        }

    async def validate_config(self):
        self.configuration.check_valid()

    async def close(self):
        await self.salesforce_client.close_session()

    async def ping(self):
        try:
            await self.salesforce_client.ping()
            self._logger.debug("Successfully connected to Salesforce.")
        except Exception as e:
            self._logger.exception(f"Error while connecting to Salesforce: {e}")
            raise

    async def get_content(self, attachment, timestamp=None, doit=False):
        # TODO implement
        return

    async def get_docs(self, filtering=None):
        # TODO implement
        yield {"foo": "bar"}, None
