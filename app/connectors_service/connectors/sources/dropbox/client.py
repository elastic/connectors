#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online."""

import json
import os
from datetime import datetime
from functools import cached_property
from urllib import parse

import aiohttp
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError
from tenacity import retry, stop_after_attempt, wait_exponential

from connectors_sdk.logger import logger

from connectors.sources.dropbox.common import (
    API_VERSION,
    BATCH_RESPONSE_COUNT,
    DEFAULT_RETRY_AFTER,
    ENDPOINTS,
    FILE,
    FILE_LIMIT,
    LIMIT,
    PAPER,
    RECEIVED_FILE,
    RETRY_COUNT,
    RETRY_INTERVAL,
    BreakingField,
    EndpointName,
    InvalidClientCredentialException,
    InvalidPathException,
    InvalidRefreshTokenException,
)
from connectors.utils import (
    CancellableSleeps,
    evaluate_timedelta,
    is_expired,
)

if "RUNNING_FTEST" in os.environ:
    logger.warning("x" * 100)
    logger.warning(
        f"DROPBOX CONNECTOR CALLS ARE REDIRECTED TO {os.environ['DROPBOX_API_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 100)
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": os.environ["DROPBOX_API_URL"],
        "FILES_FOLDERS_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
        "DOWNLOAD_BASE_URL": os.environ["DROPBOX_API_URL_V2"],
    }
else:
    BASE_URLS = {
        "ACCESS_TOKEN_BASE_URL": "https://api.dropboxapi.com/",
        "FILES_FOLDERS_BASE_URL": f"https://api.dropboxapi.com/{API_VERSION}/",
        "DOWNLOAD_BASE_URL": f"https://content.dropboxapi.com/{API_VERSION}/",
    }


class DropboxClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.path = (
            ""
            if self.configuration["path"] in ["/", None]
            else self.configuration["path"]
        )
        self.retry_count = self.configuration["retry_count"]
        self.app_key = self.configuration["app_key"]
        self.app_secret = self.configuration["app_secret"]
        self.refresh_token = self.configuration["refresh_token"]
        self.access_token = None
        self.token_expiration_time = None
        self.member_id = None
        self.root_namespace_id = None
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @retry(
        stop=stop_after_attempt(RETRY_COUNT),
        wait=wait_exponential(multiplier=1, exp_base=RETRY_INTERVAL),
        reraise=True,
    )
    async def _set_access_token(self):
        if self.token_expiration_time and (
            not isinstance(self.token_expiration_time, datetime)
        ):
            self.token_expiration_time = datetime.fromisoformat(
                self.token_expiration_time
            )
        if not is_expired(expires_at=self.token_expiration_time):
            return
        url = f"{BASE_URLS['ACCESS_TOKEN_BASE_URL']}{ENDPOINTS.get(EndpointName.ACCESS_TOKEN.value)}"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.app_key,
            "client_secret": self.app_secret,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, headers=headers, data=data) as response:
                response_data = await response.json()
                if response.status != 200:
                    self.check_errors(response=response_data)
                self.access_token = response_data["access_token"]
                self.token_expiration_time = evaluate_timedelta(
                    seconds=int(response_data["expires_in"]), time_skew=20
                )
                self._logger.debug("Access Token generated successfully")

    def check_errors(self, response):
        error_response = response.get("error")
        if error_response == "invalid_grant":
            msg = "Configured Refresh Token is invalid."
            raise InvalidRefreshTokenException(msg)
        elif error_response == "invalid_client: Invalid client_id or client_secret":
            msg = "Configured App Key or App Secret is invalid."
            raise InvalidClientCredentialException(msg)
        else:
            msg = f"Error while generating an access token. Error: {error_response}"
            raise Exception(msg)

    @cached_property
    def _get_session(self):
        self._logger.debug("Generating aiohttp client session")
        timeout = aiohttp.ClientTimeout(total=None)

        return aiohttp.ClientSession(timeout=timeout, raise_for_status=True)

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def _get_request_headers(self, file_type, url_name, **kwargs):
        kwargs = kwargs["kwargs"]
        request_headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        if file_type == FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"path": "{kwargs["path"]}"}}'
        elif file_type == PAPER:
            request_headers["Dropbox-API-Arg"] = (
                f'{{"path": "{kwargs["path"]}", "export_format": "markdown"}}'
            )
        elif file_type == RECEIVED_FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"url": "{kwargs["url"]}"}}'
        else:
            request_headers["Content-Type"] = "application/json"

        if self.member_id and (
            url_name
            not in [
                ENDPOINTS.get(EndpointName.MEMBERS.value),
                ENDPOINTS.get(EndpointName.MEMBERS_CONTINUE.value),
                ENDPOINTS.get(EndpointName.AUTHENTICATED_ADMIN.value),
                ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST.value),
                ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST_CONTINUE.value),
            ]
        ):
            request_headers["Dropbox-API-Select-User"] = self.member_id

        if url_name not in [
            ENDPOINTS.get(EndpointName.AUTHENTICATED_ADMIN.value),
            ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST.value),
            ENDPOINTS.get(EndpointName.MEMBERS.value),
        ] and (self.root_namespace_id or kwargs.get("folder_id")):
            request_headers["Dropbox-API-Path-Root"] = json.dumps(
                {
                    ".tag": "namespace_id",
                    "namespace_id": self.root_namespace_id or kwargs.get("folder_id"),
                }
            )
        return request_headers

    def _get_retry_after(self, retry, exception):
        self._logger.warning(
            f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
        )
        if retry >= self.retry_count:
            raise exception
        retry += 1
        return retry, RETRY_INTERVAL**retry

    async def _handle_client_errors(self, retry, exception):
        retry, retry_seconds = self._get_retry_after(retry=retry, exception=exception)
        match exception.status:
            case 401:
                await self._set_access_token()
            # For Dropbox APIs, 409 status code is responsible for endpoint-specific errors.
            # Refer `Errors by status code` section in the documentation: https://www.dropbox.com/developers/documentation/http/documentation
            case 409:
                msg = f"Configured Path: {self.path} is invalid or not found."
                raise InvalidPathException(msg) from exception
            case 429:
                response_headers = exception.headers
                updated_response_headers = {
                    key.lower(): value for key, value in response_headers.items()
                }
                retry_seconds = int(
                    updated_response_headers.get("retry-after", DEFAULT_RETRY_AFTER)
                )
                self._logger.warning(
                    f"Rate limited by Dropbox. Retrying after {retry_seconds} seconds"
                )
            case _:
                raise
        self._logger.debug(f"Will retry in {retry_seconds} seconds")
        await self._sleeps.sleep(retry_seconds)
        return retry

    async def api_call(self, base_url, url_name, data=None, file_type=None, **kwargs):
        retry = 1
        url = parse.urljoin(base_url, url_name)
        while True:
            try:
                await self._set_access_token()
                headers = self._get_request_headers(
                    file_type=file_type, url_name=url_name, kwargs=kwargs
                )
                async with self._get_session.post(
                    url=url, headers=headers, data=data
                ) as response:
                    yield response
                    break
            # These errors are handled in `check_errors` method
            except (InvalidClientCredentialException, InvalidRefreshTokenException):
                raise
            except ClientResponseError as exception:
                retry = await self._handle_client_errors(
                    retry=retry, exception=exception
                )
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close()
                retry, retry_seconds = self._get_retry_after(
                    retry=retry, exception=exception
                )
                await self._sleeps.sleep(retry_seconds)

    async def _paginated_api_call(
        self, base_url, breaking_field, continue_endpoint=None, **kwargs
    ):
        """Make a paginated API call for fetching Dropbox files/folders.

        Args:
            base_url (str): Base URL for dropbox APIs
            breaking_field (str): Breaking field to break the pagination
            is_shared (bool): Flag to determine the method flow. If `True` then fetching shared files, else normal files/folders.

        Yields:
            dictionary: JSON response
        """
        data = kwargs["data"]
        url_name = kwargs["url_name"]
        while True:
            try:
                async for response in self.api_call(
                    base_url=base_url,
                    url_name=url_name,
                    data=json.dumps(data),
                    folder_id=kwargs.get("folder_id"),
                ):
                    json_response = await response.json()
                    yield json_response

                    if isinstance(json_response, list):
                        return

                    # Breaking condition for pagination
                    # breaking_fields:
                    #  - "has_more" for fetching files/folders
                    #  - "cursor" for fetching shared files
                    if not json_response.get(breaking_field):
                        return

                    data = {
                        BreakingField.CURSOR.value: json_response[
                            BreakingField.CURSOR.value
                        ]
                    }
                    if continue_endpoint:
                        url_name = ENDPOINTS.get(continue_endpoint)
                    else:
                        url_name = ENDPOINTS.get(
                            EndpointName.FILES_FOLDERS_CONTINUE.value
                        )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping of fetching files/folders for url: {parse.urljoin(base_url, url_name)}. Exception: {exception}"
                )
                return

    async def ping(self, endpoint):
        return await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(endpoint),
                data=json.dumps(None),
            )
        )

    async def check_path(self, folder_id=None):
        return await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.CHECK_PATH.value),
                data=json.dumps({"path": self.path}),
                folder_id=folder_id,
            )
        )

    async def get_files_folders(self, path, folder_id):
        data = {
            "path": path,
            "recursive": True,
            "limit": FILE_LIMIT,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILES_FOLDERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
            folder_id=folder_id,
        ):
            yield result

    async def list_file_permission_without_batching(self, file_id):
        data = {
            "file": file_id,
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.FILE_MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILE_MEMBERS.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def list_file_permission_with_batching(self, file_ids):
        data = {
            "files": list(file_ids),
            "limit": BATCH_RESPONSE_COUNT,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILE_MEMBERS_BATCH.value),
            data=data,
            breaking_field=None,
        ):
            yield result

    async def list_folder_permission(self, shared_folder_id):
        data = {
            "shared_folder_id": shared_folder_id,
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.FOLDER_MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FOLDER_MEMBERS.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def get_shared_files(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.RECEIVED_FILES_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILES.value),
            data=data,
            breaking_field=BreakingField.CURSOR.value,
        ):
            yield result

    async def get_received_file_metadata(self, url):
        data = {"url": url}
        async for response in self.api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_METADATA.value),
            data=json.dumps(data),
        ):
            yield response

    async def download_files(self, path, folder_id):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.DOWNLOAD.value),
            file_type=FILE,
            path=path,
            folder_id=folder_id,
        ):
            yield response

    async def download_shared_file(self, url):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.RECEIVED_FILE_DOWNLOAD.value),
            file_type=RECEIVED_FILE,
            url=url,
        ):
            yield response

    async def download_paper_files(self, path, folder_id=None):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.PAPER_FILE_DOWNLOAD.value),
            file_type=PAPER,
            path=path,
            folder_id=folder_id,
        ):
            yield response

    async def search_files_folders(self, rule):
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.SEARCH.value),
            data=rule,
            breaking_field=BreakingField.HAS_MORE.value,
            continue_endpoint=EndpointName.SEARCH_CONTINUE.value,
        ):
            yield result

    async def list_members(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.MEMBERS_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.MEMBERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result

    async def get_team_folder_list(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint=EndpointName.TEAM_FOLDER_LIST_CONTINUE.value,
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.TEAM_FOLDER_LIST.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result
