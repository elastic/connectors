#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""OneDrive source module responsible to fetch documents from OneDrive.
"""
import asyncio
import os
from datetime import datetime, timedelta
from functools import cached_property, partial
from urllib import parse

import aiofiles
import aiohttp
import fastjsonschema
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ClientResponseError, ServerConnectionError
from wcmatch import glob

from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    convert_to_b64,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
CHUNK_SIZE = 1024
FILE_SIZE_LIMIT = 10485760  # ~10 Megabytes
FETCH_SIZE = 999
DEFAULT_PARALLEL_CONNECTION_COUNT = 15
REQUEST_TIMEOUT = 300
FILE = "file"
FOLDER = "folder"

USERS = "users"
DELTA = "delta"
PING = "ping"
ITEM_FIELDS = "id,name,lastModifiedDateTime,content.downloadUrl,createdDateTime,size,webUrl,parentReference,file,folder"

ENDPOINTS = {
    PING: "drives",
    USERS: "users",
    DELTA: "users/{user_id}/drive/root/delta",
}

if "OVERRIDE_URL" in os.environ:
    logger.warning("x" * 50)
    logger.warning(
        f"ONEDRIVE CONNECTOR CALLS ARE REDIRECTED TO {os.environ['OVERRIDE_URL']}"
    )
    logger.warning("IT'S SUPPOSED TO BE USED ONLY FOR TESTING")
    logger.warning("x" * 50)
    override_url = os.environ["OVERRIDE_URL"]
    BASE_URL = override_url
    GRAPH_API_AUTH_URL = override_url
else:
    BASE_URL = "https://graph.microsoft.com/v1.0/"
    GRAPH_API_AUTH_URL = "https://login.microsoftonline.com"


class TokenRetrievalError(Exception):
    """Exception class to notify that fetching of access token was not successful."""

    pass


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
    pass


class OneDriveAdvancedRulesValidator(AdvancedRulesValidator):
    RULES_OBJECT_SCHEMA_DEFINITION = {
        "type": "object",
        "properties": {
            "skipFilesWithExtensions": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string"},
            },
            "parentPathPattern": {"type": "string", "minLength": 1},
            "owners": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "format": "email", "minLength": 1},
            },
        },
        "minProperties": 1,
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

        try:
            OneDriveAdvancedRulesValidator.SCHEMA(advanced_rules)
        except fastjsonschema.JsonSchemaValueException as e:
            return SyncRuleValidationResult(
                rule_id=SyncRuleValidationResult.ADVANCED_RULES,
                is_valid=False,
                validation_message=e.message,
            )

        return SyncRuleValidationResult.valid_result(
            SyncRuleValidationResult.ADVANCED_RULES
        )


class AccessToken:
    """Class for handling access token for Microsoft Graph APIs"""

    def __init__(self, configuration):
        self.tenant_id = configuration["tenant_id"]
        self.client_id = configuration["client_id"]
        self.client_secret = configuration["client_secret"]
        self._token_cache = CacheWithTimeout()

    async def get(self):
        """Get bearer token required for API call.

        If the token is not present in the cache or expired,
        it calls _set_access_token that sends a POST request
        to Microsoft for generating a new access token.

        Returns:
            str: Access Token
        """
        if cached_value := self._token_cache.get_value():
            return cached_value
        try:
            await self._set_access_token()
        except ClientResponseError as e:
            match e.status:
                case 400:
                    raise TokenRetrievalError(
                        "Failed to fetch access token. Please verify that provided Tenant ID, Client ID are correct."
                    ) from e
                case 401:
                    raise TokenRetrievalError(
                        "Failed to fetch access token. Please check if Client Secret is valid."
                    ) from e
                case _:
                    raise TokenRetrievalError(
                        f"Failed to fetch access token. Response Status: {e.status}, Message: {e.message}"
                    ) from e
        return self.access_token

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        """Generate access token with configuration fields and stores it in the cache"""
        url = f"{GRAPH_API_AUTH_URL}/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        async with aiohttp.request(
            method="POST", url=url, data=data, headers=headers, raise_for_status=True
        ) as response:
            token_reponse = await response.json()
            self.access_token = token_reponse["access_token"]
            self.token_expires_at = datetime.utcnow() + timedelta(
                seconds=int(token_reponse.get("expires_in", 0))
            )
            self._token_cache.set_value(self.access_token, self.token_expires_at)


class OneDriveClient:
    """Client Class for API calls to OneDrive"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self.retry_count = self.configuration["retry_count"]
        self._logger = logger
        self.token = AccessToken(configuration=configuration)

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        """Generate base client session with configuration fields
        Returns:
            ClientSession: Base client session
        """
        connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        return aiohttp.ClientSession(
            timeout=timeout, raise_for_status=True, connector=connector
        )

    async def close_session(self):
        self._sleeps.cancel()
        await self.session.close()
        del self.session

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def get(self, url):
        access_token = await self.token.get()
        headers = {"authorization": f"Bearer {access_token}"}
        try:
            async with self.session.get(url=url, headers=headers) as response:
                yield response
        except ServerConnectionError:
            await self.close_session()
            raise
        except ClientResponseError as e:
            if e.status == 429 or e.status == 503:
                response_headers = e.headers or {}
                retry_seconds = DEFAULT_RETRY_SECONDS
                if "Retry-After" in response_headers:
                    try:
                        retry_seconds = int(response_headers["Retry-After"])
                    except (TypeError, ValueError) as exception:
                        self._logger.error(
                            f"Error while reading value of retry-after header {exception}. Using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                        )
                else:
                    self._logger.warning(
                        f"Rate Limited but Retry-After header is not found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                    )
                self._logger.debug(
                    f"Rate Limit reached: retry in {retry_seconds} seconds"
                )

                await self._sleeps.sleep(retry_seconds)
                raise ThrottledError from e
            elif e.status == 404:
                raise NotFound from e
            elif e.status == 500:
                raise InternalServerError from e
            else:
                raise

    async def paginated_api_call(self, url, params=None, fetch_size=FETCH_SIZE):
        if params is None:
            params = {}
        params["$top"] = fetch_size
        params = "&".join(f"{key}={val}" for key, val in params.items())

        url = f"{url}?{params}"
        while True:
            try:
                async for response in self.get(
                    url=url,
                ):
                    response_json = await response.json()
                    result = response_json["value"]

                    if len(result) == 0:
                        return

                    yield result

                    url = response_json.get("@odata.nextLink")
                    if not url:
                        return
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for {url}. Exception: {exception}."
                )
                break

    async def list_users(self):
        url = parse.urljoin(BASE_URL, ENDPOINTS[USERS])

        async for response in self.paginated_api_call(url):
            for user_detail in response:
                yield user_detail

    async def get_owned_files(self, user_id, skipped_extensions=None, pattern=""):
        params = {"$select": ITEM_FIELDS}
        delta_endpoint = ENDPOINTS[DELTA].format(user_id=user_id)

        url = parse.urljoin(BASE_URL, delta_endpoint)
        async for response in self.paginated_api_call(url, params):
            for file in response:
                if file.get("name", "") != "root":
                    parent_path = file.get("parentReference", {}).get("path")
                    is_match = glob.globmatch(parent_path, pattern, flags=glob.GLOBSTAR)
                    has_skipped_extension = os.path.splitext(file["name"])[-1] in (
                        skipped_extensions or []
                    )
                    if has_skipped_extension or (pattern and not is_match):
                        continue
                    else:
                        yield file, file.get("@microsoft.graph.downloadUrl")


class OneDriveDataSource(BaseDataSource):
    """OneDrive"""

    name = "OneDrive"
    service_type = "onedrive"
    advanced_rules_enabled = True

    def __init__(self, configuration):
        """Setup the connection to OneDrive

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.configuration = configuration
        self.concurrent_downloads = self.configuration["concurrent_downloads"]

    @cached_property
    def client(self):
        return OneDriveClient(self.configuration)

    def _set_internal_logger(self):
        self.client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for OneDrive

        Returns:
            dictionary: Default configuration.
        """
        return {
            "client_id": {
                "label": "Azure application Client ID",
                "order": 1,
                "type": "str",
            },
            "client_secret": {
                "label": "Azure application Client Secret",
                "order": 2,
                "sensitive": True,
                "type": "str",
            },
            "tenant_id": {
                "label": "Azure application Tenant ID",
                "order": 3,
                "type": "str",
            },
            "retry_count": {
                "default_value": 3,
                "display": "numeric",
                "label": "Maximum retries per request",
                "order": 4,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "concurrent_downloads": {
                "default_value": DEFAULT_PARALLEL_CONNECTION_COUNT,
                "display": "numeric",
                "label": "Maximum concurrent downloads",
                "order": 5,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }

    def tweak_bulk_options(self, options):
        """Tweak bulk options as per concurrent downloads support by ServiceNow

        Args:
            options (dict): Config bulker options.
        """

        options["concurrent_downloads"] = self.concurrent_downloads

    def advanced_rules_validators(self):
        return [OneDriveAdvancedRulesValidator(self)]

    async def close(self):
        """Closes unclosed client session"""
        await self.client.close_session()

    async def ping(self):
        """Verify the connection with OneDrive"""
        try:
            url = parse.urljoin(BASE_URL, ENDPOINTS[PING])
            await anext(self.client.get(url=url))
            self._logger.info("Successfully connected to OneDrive")
        except Exception:
            self._logger.exception("Error while connecting to OneDrive")
            raise

    def _pre_checks_for_get_content(
        self, attachment_extension, attachment_name, attachment_size
    ):
        if attachment_extension == "":
            self._logger.warning(
                f"Files without extension are not supported, skipping {attachment_name}."
            )
            return False

        if attachment_extension.lower() not in TIKA_SUPPORTED_FILETYPES:
            self._logger.warning(
                f"Files with the extension {attachment_extension} are not supported, skipping {attachment_name}."
            )
            return False

        if attachment_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {attachment_size} of file {attachment_name} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return
        return True

    async def _get_document_with_content(self, attachment_name, document, url):
        temp_filename = ""

        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self.client.get(url=url):
                async for data in response.content.iter_chunked(n=CHUNK_SIZE):
                    await async_buffer.write(data)
            temp_filename = str(async_buffer.name)

        self._logger.debug(
            f"Download completed for file: {attachment_name}. Calling convert_to_b64"
        )
        await asyncio.to_thread(
            convert_to_b64,
            source=temp_filename,
        )
        async with aiofiles.open(file=temp_filename, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            document["_attachment"] = (await target_file.read()).strip()
        try:
            await remove(temp_filename)
        except Exception as exception:
            self._logger.warning(
                f"Could not remove file: {temp_filename}. Error: {exception}"
            )
        return document

    async def get_content(self, file, download_url, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            file (dict): File metadata
            download_url (str): Download URL for the file
            timestamp (timestamp, optional): Timestamp of file last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and file content
        """

        attachment_size = int(file["size"])
        if not (doit and attachment_size > 0):
            return

        attachment_name = file["title"]

        attachment_extension = (
            attachment_name[attachment_name.rfind(".") :]  # noqa
            if "." in attachment_name
            else ""
        )

        if not self._pre_checks_for_get_content(
            attachment_extension=attachment_extension,
            attachment_name=attachment_name,
            attachment_size=attachment_size,
        ):
            return

        self._logger.debug(f"Downloading {attachment_name}")

        document = {
            "_id": file["_id"],
            "_timestamp": file["_timestamp"],
        }

        return await self._get_document_with_content(
            attachment_name=attachment_name,
            document=document,
            url=download_url,
        )

    def prepare_doc(self, file):
        return {
            "type": FILE if file.get(FILE) else FOLDER,
            "title": file.get("name"),
            "_id": file.get("id"),
            "_timestamp": file.get("lastModifiedDateTime"),
            "created_at": file.get("createdDateTime"),
            "size": file.get("size"),
            "url": file.get("webUrl"),
        }

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch OneDrive objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()

            user_mail_id_map = {}
            async for user in self.client.list_users():
                user_mail_id_map[user["mail"]] = user["id"]

            for query_info in advanced_rules:
                skipped_extensions = query_info.get("skipFilesWithExtensions")
                user_mails = query_info.get("owners")
                if user_mails is None:
                    user_mails = user_mail_id_map.keys()

                pattern = query_info.get("parentPathPattern", "")

                for mail in user_mails:
                    user_id = user_mail_id_map.get(mail)
                    async for entity, download_url in self.client.get_owned_files(
                        user_id, skipped_extensions, pattern
                    ):
                        entity = self.prepare_doc(entity)
                        if entity["type"] == FILE and download_url:
                            yield entity, partial(
                                self.get_content, entity.copy(), download_url
                            )
                        else:
                            yield entity, None
        else:
            async for user in self.client.list_users():
                user_id = user.get("id")

                async for entity, download_url in self.client.get_owned_files(user_id):
                    entity = self.prepare_doc(entity)
                    if entity["type"] == FILE and download_url:
                        yield entity, partial(
                            self.get_content, entity.copy(), download_url
                        )
                    else:
                        yield entity, None
