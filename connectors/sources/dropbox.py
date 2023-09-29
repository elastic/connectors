#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Dropbox source module responsible to fetch documents from Dropbox online.
"""
import json
import os
from datetime import datetime
from enum import Enum
from functools import cached_property, partial
from urllib import parse

import aiohttp
import fastjsonschema
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    evaluate_timedelta,
    is_expired,
    iso_utc,
    retryable,
)

RETRY_COUNT = 3
DEFAULT_RETRY_AFTER = 300  # seconds
RETRY_INTERVAL = 2
MAX_CONCURRENT_DOWNLOADS = 100
LIMIT = 300  # Limit for fetching shared files per call
PAPER = "paper"
FILE = "File"
FOLDER = "Folder"
RECEIVED_FILE = "Received File"

API_VERSION = 2

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


class EndpointName(Enum):
    ACCESS_TOKEN = "access_token"
    PING = "ping"
    CHECK_PATH = "check_path"
    FILES_FOLDERS = "files_folders"
    FILES_FOLDERS_CONTINUE = "files_folders_continue"
    RECEIVED_FILES = "received_files"
    RECEIVED_FILES_CONTINUE = "received_files_continue"
    RECEIVED_FILE_METADATA = "received_files_metadata"
    DOWNLOAD = "download"
    PAPER_FILE_DOWNLOAD = "paper_file_download"
    RECEIVED_FILE_DOWNLOAD = "received_files_download"
    SEARCH = "search"
    SEARCH_CONTINUE = "search_continue"


ENDPOINTS = {
    EndpointName.ACCESS_TOKEN.value: "oauth2/token",
    EndpointName.PING.value: "users/get_current_account",
    EndpointName.CHECK_PATH.value: "files/get_metadata",
    EndpointName.FILES_FOLDERS.value: "files/list_folder",
    EndpointName.FILES_FOLDERS_CONTINUE.value: "files/list_folder/continue",
    EndpointName.RECEIVED_FILES.value: "sharing/list_received_files",
    EndpointName.RECEIVED_FILES_CONTINUE.value: "sharing/list_received_files/continue",
    EndpointName.RECEIVED_FILE_METADATA.value: "sharing/get_shared_link_metadata",
    EndpointName.DOWNLOAD.value: "files/download",
    EndpointName.PAPER_FILE_DOWNLOAD.value: "files/export",
    EndpointName.RECEIVED_FILE_DOWNLOAD.value: "sharing/get_shared_link_file",
    EndpointName.SEARCH.value: "files/search_v2",
    EndpointName.SEARCH_CONTINUE.value: "files/search/continue_v2",
}


class InvalidClientCredentialException(Exception):
    pass


class InvalidRefreshTokenException(Exception):
    pass


class InvalidPathException(Exception):
    pass


class InvalidDownloadFormatException(Exception):
    pass


class BreakingField(Enum):
    CURSOR = "cursor"
    HAS_MORE = "has_more"


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
        self._logger = logger

    def set_logger(self, logger_):
        self._logger = logger_

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
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
            raise InvalidRefreshTokenException("Configured Refresh Token is invalid.")
        elif error_response == "invalid_client: Invalid client_id or client_secret":
            raise InvalidClientCredentialException(
                "Configured App Key or App Secret is invalid."
            )
        else:
            raise Exception(
                f"Error while generating an access token. Error: {error_response}"
            )

    @cached_property
    def _get_session(self):
        self._logger.debug("Generating aiohttp client session")
        timeout = aiohttp.ClientTimeout(total=None)

        return aiohttp.ClientSession(timeout=timeout, raise_for_status=True)

    async def close(self):
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    def _get_request_headers(self, file_type, **kwargs):
        kwargs = kwargs["kwargs"]
        request_headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        if file_type == FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"path": "{kwargs["path"]}"}}'
        elif file_type == PAPER:
            request_headers[
                "Dropbox-API-Arg"
            ] = f'{{"path": "{kwargs["path"]}", "export_format": "markdown"}}'
        elif file_type == RECEIVED_FILE:
            request_headers["Dropbox-API-Arg"] = f'{{"url": "{kwargs["url"]}"}}'
        else:
            request_headers["Content-Type"] = "application/json"
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
                raise InvalidPathException(
                    f"Configured Path: {self.path} is invalid or not found."
                ) from exception
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
                headers = self._get_request_headers(file_type=file_type, kwargs=kwargs)
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
                ):
                    json_response = await response.json()
                    yield json_response

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
                    if continue_endpoint == "shared_file":
                        url_name = ENDPOINTS.get(
                            EndpointName.RECEIVED_FILES_CONTINUE.value
                        )
                    elif continue_endpoint == "search_file":
                        url_name = ENDPOINTS.get(EndpointName.SEARCH_CONTINUE.value)
                    else:
                        url_name = ENDPOINTS.get(
                            EndpointName.FILES_FOLDERS_CONTINUE.value
                        )
            except Exception as exception:
                self._logger.warning(
                    f"Skipping of fetching files/folders for url: {parse.urljoin(base_url, url_name)}. Exception: {exception}"
                )
                return

    async def ping(self):
        await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.PING.value),
                data=json.dumps(None),
            )
        )

    async def check_path(self):
        return await anext(
            self.api_call(
                base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
                url_name=ENDPOINTS.get(EndpointName.CHECK_PATH.value),
                data=json.dumps({"path": self.path}),
            )
        )

    async def get_files_folders(self, path):
        data = {
            "path": path,
            "recursive": True,
        }
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.FILES_FOLDERS.value),
            data=data,
            breaking_field=BreakingField.HAS_MORE.value,
        ):
            yield result

    async def get_shared_files(self):
        data = {
            "limit": LIMIT,
        }
        async for result in self._paginated_api_call(
            continue_endpoint="shared_file",
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

    async def download_files(self, path):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.DOWNLOAD.value),
            file_type=FILE,
            path=path,
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

    async def download_paper_files(self, path):
        async for response in self.api_call(
            base_url=BASE_URLS["DOWNLOAD_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.PAPER_FILE_DOWNLOAD.value),
            file_type=PAPER,
            path=path,
        ):
            yield response

    async def search_files_folders(self, rule):
        async for result in self._paginated_api_call(
            base_url=BASE_URLS["FILES_FOLDERS_BASE_URL"],
            url_name=ENDPOINTS.get(EndpointName.SEARCH.value),
            data=rule,
            breaking_field=BreakingField.HAS_MORE.value,
            continue_endpoint="search_file",
        ):
            yield result


class DropBoxAdvancedRulesValidator(AdvancedRulesValidator):
    FILE_CATEGORY_DEFINITION = {
        "type": "object",
        "properties": {
            ".tag": {"type": "string", "minLength": 1},
        },
    }

    OPTIONS_DEFINITION = {
        "type": "object",
        "properties": {
            "path": {"type": "string", "minLength": 1},
            "max_results": {"type": "string", "minLength": 1},
            "order_by": FILE_CATEGORY_DEFINITION,
            "file_status": FILE_CATEGORY_DEFINITION,
            "account_id": {"type": "string", "minLength": 1},
            "file_extensions": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
            },
            "file_categories": {
                "type": "array",
                "items": FILE_CATEGORY_DEFINITION,
                "minItems": 1,
            },
        },
    }

    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "match_field_options": {"type": "object"},
            "options": OPTIONS_DEFINITION,
            "query": {"type": "string", "minLength": 1},
        },
        "required": ["query"],
        "additionalProperties": False,
    }

    SCHEMA_DEFINITION = {"type": "array", "items": RULES_OBJECT_SCHEMA_DEFINITION}

    SCHEMA = fastjsonschema.compile(definition=SCHEMA_DEFINITION)

    def __init__(self, source):
        self.source = source

    async def validate(self, advanced_rules):
        if len(advanced_rules) == 0:
            return SyncRuleValidationResult.valid_result(
                SyncRuleValidationResult.ADVANCED_RULES
            )

        return await self._remote_validation(advanced_rules)

    @retryable(
        retries=RETRY_COUNT,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _remote_validation(self, advanced_rules):
        try:
            DropBoxAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        invalid_paths = []
        for rule in advanced_rules:
            self.source.dropbox_client.path = rule.get("options", {}).get("path")
            try:
                if self.source.dropbox_client.path:
                    await self.source.dropbox_client.check_path()
            except InvalidPathException:
                invalid_paths.append(self.source.dropbox_client.path)

        if len(invalid_paths) > 0:
            return SyncRuleValidationResult(
                SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=f"Invalid paths '{', '.join(invalid_paths)}'.",
            )
        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class DropboxDataSource(BaseDataSource):
    """Dropbox"""

    name = "Dropbox"
    service_type = "dropbox"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Setup the connection to the Dropbox

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.dropbox_client = DropboxClient(configuration=configuration)
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    def _set_internal_logger(self):
        self.dropbox_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Dropbox

        Returns:
            dictionary: Default configuration.
        """
        return {
            "path": {
                "label": "Path to fetch files/folders",
                "order": 1,
                "required": False,
                "tooltip": "Path is ignored when Advanced Sync Rules are used.",
                "type": "str",
            },
            "app_key": {
                "label": "App Key",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "app_secret": {
                "label": "App secret",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "refresh_token": {
                "label": "Refresh token",
                "sensitive": True,
                "order": 4,
                "type": "str",
            },
            "retry_count": {
                "default_value": RETRY_COUNT,
                "display": "numeric",
                "label": "Retries per request",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": MAX_CONCURRENT_DOWNLOADS,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 6,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "value": False,
            },
        }

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured path is available in Dropbox."""

        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        try:
            if self.dropbox_client.path not in ["", None]:
                await self.dropbox_client.check_path()
        except InvalidPathException:
            raise
        except Exception as exception:
            raise Exception(
                f"Error while validating path: {self.dropbox_client.path}. Error: {exception}"
            ) from exception

    def advanced_rules_validators(self):
        return [DropBoxAdvancedRulesValidator(self)]

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by dropbox

        Args:
            options (dictionary): Config bulker options
        """
        options["concurrent_downloads"] = self.concurrent_downloads

    async def close(self):
        await self.dropbox_client.close()

    async def ping(self):
        await self.dropbox_client.ping()
        self._logger.info("Successfully connected to Dropbox")

    async def get_content(
        self, attachment, is_shared=False, timestamp=None, doit=False
    ):
        """Extracts the content for allowed file types.

        Args:
            attachment (object): Attachment object
            is_shared (boolean, optional): Flag to check if want a content for shared file. Defaults to False.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        file_size = int(attachment["size"])
        if not (doit and file_size > 0):
            return

        filename = attachment["name"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        download_func = self.download_func(is_shared, attachment, filename)
        if not download_func:
            self._logger.warning(
                f"Skipping the file: {filename} since it is not in the downloadable format."
            )
            return

        document = {
            "_id": attachment["id"],
            "_timestamp": attachment["server_modified"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                download_func,
            ),
        )

    def download_func(self, is_shared, attachment, filename):
        if is_shared:
            return partial(
                self.dropbox_client.download_shared_file, url=attachment["url"]
            )
        elif attachment["is_downloadable"]:
            return partial(
                self.dropbox_client.download_files, path=attachment["path_display"]
            )
        elif filename.split(".")[-1] == PAPER:
            return partial(
                self.dropbox_client.download_paper_files,
                path=attachment["path_display"],
            )
        else:
            return

    def _adapt_dropbox_doc_to_es_doc(self, response):
        is_file = response.get(".tag") == "file"
        if is_file and response.get("name").split(".")[-1] == PAPER:
            timestamp = response.get("client_modified")
        else:
            timestamp = response.get("server_modified") if is_file else iso_utc()
        return {
            "_id": response.get("id"),
            "type": FILE if is_file else FOLDER,
            "name": response.get("name"),
            "file_path": response.get("path_display"),
            "size": response.get("size") if is_file else 0,
            "_timestamp": timestamp,
        }

    def _adapt_dropbox_shared_file_doc_to_es_doc(self, response):
        return {
            "_id": response.get("id"),
            "type": FILE,
            "name": response.get("name"),
            "url": response.get("url"),
            "size": response.get("size"),
            "_timestamp": response.get("server_modified"),
        }

    async def _fetch_files_folders(self, path):
        async for response in self.dropbox_client.get_files_folders(path=path):
            for entry in response.get("entries"):
                yield self._adapt_dropbox_doc_to_es_doc(response=entry), entry

    async def _fetch_shared_files(self):
        async for response in self.dropbox_client.get_shared_files():
            for entry in response.get("entries"):
                async for metadata in self.dropbox_client.get_received_file_metadata(
                    url=entry["preview_url"]
                ):
                    json_metadata = await metadata.json()
                    yield self._adapt_dropbox_shared_file_doc_to_es_doc(
                        response=json_metadata
                    ), json_metadata

    async def advanced_sync(self, rule):
        async for response in self.dropbox_client.search_files_folders(rule=rule):
            for entry in response.get("matches"):
                data = entry.get("metadata", {}).get("metadata")
                if data.get("preview_url") and not data.get("export_info"):
                    async for metadata in self.dropbox_client.get_received_file_metadata(
                        url=data["preview_url"]
                    ):
                        json_metadata = await metadata.json()
                        yield self._adapt_dropbox_shared_file_doc_to_es_doc(
                            response=json_metadata
                        ), json_metadata
                else:
                    yield self._adapt_dropbox_doc_to_es_doc(response=data), data

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch dropbox objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        def document_tuple(document, attachment):
            if document.get("type") == FILE:
                if document.get("url"):
                    return document, partial(
                        self.get_content, attachment=attachment, is_shared=True
                    )
                else:
                    return document, partial(self.get_content, attachment=attachment)
            else:
                return document, None

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            for rule in advanced_rules:
                self._logger.debug(f"Fetching files using advanced sync rule: {rule}")

                async for document, attachment in self.advanced_sync(rule=rule):
                    yield document_tuple(document=document, attachment=attachment)
        else:
            async for document, attachment in self._fetch_files_folders(
                path=self.dropbox_client.path
            ):
                yield document_tuple(document=document, attachment=attachment)

            async for document, attachment in self._fetch_shared_files():
                yield document_tuple(document=document, attachment=attachment)
