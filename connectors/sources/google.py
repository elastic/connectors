#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from enum import Enum

from aiogoogle import Aiogoogle, HTTPError
from aiogoogle.auth.creds import ServiceAccountCreds

from connectors.logger import logger
from connectors.utils import RetryStrategy, retryable

RETRIES = 3
RETRY_INTERVAL = 2
FILE_SIZE_LIMIT = 10485760  # ~ 10 Megabytes
DEFAULT_TIMEOUT = 1 * 60  # 1 min
ONE_HUNDRED_ITEMS = 100


class UserFields(Enum):
    EMAIL = "primaryEmail"
    CREATION_DATE = "creationTime"


class MessageFields(Enum):
    ID = "id"
    CREATION_DATE = "internalDate"
    FULL_MESSAGE = "raw"


class GoogleServiceAccountClient:
    """A Google client to handle api calls made to the Google Workspace APIs using a service account."""

    def __init__(self, json_credentials, api, api_version, scopes, api_timeout):
        """Initialize the ServiceAccountCreds class using which api calls will be made.
        Args:
            json_credentials (dict): Service account credentials json.
        """
        self.service_account_credentials = ServiceAccountCreds(
            scopes=scopes,
            **json_credentials,
        )
        self.api = api
        self.api_version = api_version
        self.api_timeout = api_timeout
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    async def api_call_paged(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a paged GET call to a Google Workspace API.
        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
        Raises:
            exception: An instance of an exception class.
        Yields:
            async generator: Paginated response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            first_page_with_next_attached = await google_client.as_service_account(
                method_object(**kwargs),
                full_res=True,
                timeout=self.api_timeout,
            )

            if first_page_with_next_attached.content is not None:
                async for page_items in first_page_with_next_attached:
                    yield page_items

        async for item in self._execute_api_call(resource, method, _call_api, kwargs):
            yield item

    async def api_call(
        self,
        resource,
        method,
        **kwargs,
    ):
        """Make a non-paged GET call to Google Workspace API.
        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
        Raises:
            exception: An instance of an exception class.
        Yields:
            dict: Response returned by the resource method.
        """

        async def _call_api(google_client, method_object, kwargs):
            yield await google_client.as_service_account(
                method_object(**kwargs), timeout=self.api_timeout
            )

        return await anext(self._execute_api_call(resource, method, _call_api, kwargs))

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _execute_api_call(self, resource, method, call_api_func, kwargs):
        """Execute the API call with common try/except logic.
        Args:
            resource (aiogoogle.resource.Resource): Resource name for which the API call will be made.
            method (aiogoogle.resource.Method): Method available for the resource.
            call_api_func (function): Function to call the API with specific logic.
            kwargs: Additional arguments for the API call.
        Raises:
            exception: An instance of an exception class.
        Yields:
            async generator: Response returned by the resource method.
        """
        try:
            async with Aiogoogle(
                service_account_creds=self.service_account_credentials
            ) as google_client:
                workspace_client = await google_client.discover(
                    api_name=self.api, api_version=self.api_version
                )

                if isinstance(resource, list):
                    resource_object = getattr(workspace_client, resource[0])
                    for nested_resource in resource[1:]:
                        resource_object = getattr(resource_object, nested_resource)
                else:
                    resource_object = getattr(workspace_client, resource)
                method_object = getattr(resource_object, method)

                async for item in call_api_func(google_client, method_object, kwargs):
                    yield item

        except AttributeError as exception:
            self._logger.error(
                f"Error occurred while generating the resource/method object for an API call. Error: {exception}"
            )
            raise
        except HTTPError as exception:
            self._logger.warning(
                f"Response code: {exception.res.status_code} Exception: {exception}."
            )
            raise
        except Exception as exception:
            self._logger.warning(f"Exception: {exception}.")
            raise


def remove_universe_domain(json_credentials):
    if "universe_domain" in json_credentials:
        json_credentials.pop("universe_domain")


class GoogleDirectoryClient:
    def __init__(self, json_credentials, customer_id, timeout=DEFAULT_TIMEOUT):
        remove_universe_domain(json_credentials)
        self._customer_id = customer_id
        self._client = GoogleServiceAccountClient(
            json_credentials=json_credentials,
            api="admin",
            api_version="directory_v1",
            scopes=["https://www.googleapis.com/auth/admin.directory.user.readonly"],
            api_timeout=timeout,
        )

    def set_logger(self, logger_):
        self._logger = logger_

    async def ping(self):
        try:
            await self._client.api_call(
                resource="users",
                method="list",
                maxResults=1,
                customer=self._customer_id,
            )
        except Exception:
            raise

    async def users(self):
        users_fields = f"{UserFields.EMAIL.value},{UserFields.CREATION_DATE.value}"

        async for page in self._client.api_call_paged(
            resource="users",
            method="list",
            fields=f"nextPageToken,users({users_fields})",
            pageSize=ONE_HUNDRED_ITEMS,
            customer=self._customer_id,
        ):
            for user in page.get("users", []):
                yield user


class GMailClient:
    def __init__(self, json_credentials, customer_id, subject, timeout=DEFAULT_TIMEOUT):
        remove_universe_domain(json_credentials)

        # This override is needed to be able to fetch the messages for the corresponding user, otherwise we get a 403 Forbidden (see: https://issuetracker.google.com/issues/290567932)
        json_credentials["subject"] = subject
        self.user = subject
        self._customer_id = customer_id
        self._client = GoogleServiceAccountClient(
            json_credentials=json_credentials,
            api="gmail",
            api_version="v1",
            scopes=["https://www.googleapis.com/auth/gmail.readonly"],
            api_timeout=timeout,
        )

    def set_logger(self, logger_):
        self._logger = logger_

    async def ping(self):
        try:
            await self._client.api_call(
                resource="users", method="getProfile", userId=self.user
            )
        except Exception:
            raise

    async def messages(self, query=None, pageSize=ONE_HUNDRED_ITEMS):
        fields = "id"

        async for page in self._client.api_call_paged(
            resource=["users", "messages"],
            method="list",
            userId=self.user,
            q=query,
            fields=f"nextPageToken,messages({fields})",
            pageSize=pageSize,
        ):
            for message in page.get("messages", []):
                yield message

    async def message(self, id_):
        fields = "raw,internalDate"

        return await self._client.api_call(
            resource=["users", "messages"],
            method="get",
            format="raw",
            userId=self.user,
            id=id_,
            fields=fields,
        )
