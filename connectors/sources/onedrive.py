#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""OneDrive source module responsible to fetch documents from OneDrive.
"""
import asyncio
import json
import os
from datetime import datetime, timedelta
from functools import cached_property, partial
from urllib import parse

import aiohttp
import fastjsonschema
from aiohttp.client_exceptions import (
    ClientPayloadError,
    ClientResponseError,
    ServerConnectionError,
)
from wcmatch import glob

from connectors.access_control import (
    ACCESS_CONTROL,
    es_access_control_query,
    prefix_identity,
)
from connectors.filtering.validation import (
    AdvancedRulesValidator,
    SyncRuleValidationResult,
)
from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    CacheWithTimeout,
    CancellableSleeps,
    RetryStrategy,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
FETCH_SIZE = 999
DEFAULT_PARALLEL_CONNECTION_COUNT = 15
REQUEST_TIMEOUT = 300
FILE = "file"
FOLDER = "folder"

USERS = "users"
GROUPS = "groups"
PERMISSIONS = "permissions"
DELTA = "delta"
PING = "ping"
BATCH = "batch"
ITEM_FIELDS = "id,name,lastModifiedDateTime,content.downloadUrl,createdDateTime,size,webUrl,parentReference,file,folder"

ENDPOINTS = {
    PING: "drives",
    USERS: "users",
    GROUPS: "users/{user_id}/transitiveMemberOf",
    PERMISSIONS: "users/{user_id}/drive/items/{item_id}/permissions",
    DELTA: "users/{user_id}/drive/root/delta",
    BATCH: "$batch",
}

GRAPH_API_MAX_BATCH_SIZE = 20

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


def _prefix_email(email):
    return prefix_identity("email", email)


def _prefix_user(user):
    return prefix_identity("user", user)


def _prefix_user_id(user_id):
    return prefix_identity("user_id", user_id)


def _prefix_group(group):
    return prefix_identity("group", group)


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
                    msg = "Failed to fetch access token. Please verify that provided Tenant ID, Client ID are correct."
                    raise TokenRetrievalError(msg) from e
                case 401:
                    msg = "Failed to fetch access token. Please check if Client Secret is valid."
                    raise TokenRetrievalError(msg) from e
                case _:
                    msg = f"Failed to fetch access token. Response Status: {e.status}, Message: {e.message}"
                    raise TokenRetrievalError(msg) from e
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
        self._logger.debug("Creating a client session")
        connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        return aiohttp.ClientSession(
            timeout=timeout,
            raise_for_status=True,
            connector=connector,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
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
    async def get(self, url, header=None):
        self._logger.debug(f"Making a GET call to url: {url}")
        access_token = await self.token.get()
        headers = {"authorization": f"Bearer {access_token}"}
        if header:
            headers |= header
        try:
            async with self.session.get(url=url, headers=headers) as response:
                yield response
        except ServerConnectionError:
            self._logger.exception(f"Getting ServerConnectionError for url: {url}")
            await self.close_session()
            raise
        except ClientResponseError as e:
            await self._handle_client_side_errors(e)

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def post(self, url, payload=None):
        self._logger.debug(f"Making a POST call to url: {url} with payload: {payload}")
        access_token = await self.token.get()
        headers = {
            "authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }
        try:
            async with self.session.post(url, headers=headers, data=payload) as resp:
                yield resp

        except ClientResponseError as e:
            await self._handle_client_side_errors(e)
        except ClientPayloadError as e:
            retry_seconds = DEFAULT_RETRY_SECONDS
            response_headers = e.headers or {}
            if "Retry-After" in response_headers:
                try:
                    retry_seconds = int(response_headers["Retry-After"])
                except (TypeError, ValueError) as exception:
                    self._logger.error(
                        f"Error while reading value of retry-after header {exception}. Using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                    )
            await self._sleeps.sleep(retry_seconds)
            raise

    async def _handle_client_side_errors(self, e):
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
            self._logger.debug(f"Rate Limit reached: retry in {retry_seconds} seconds")

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError from e
        elif e.status == 404:
            raise NotFound from e
        elif e.status == 500:
            raise InternalServerError from e
        else:
            raise

    async def paginated_api_call(
        self, url, params=None, fetch_size=FETCH_SIZE, header=None
    ):
        if params is None:
            params = {}
        params["$top"] = fetch_size
        params = "&".join(f"{key}={val}" for key, val in params.items())

        self._logger.info(
            f"Started pagination for url: {url} with parameters: {params}"
        )
        url = f"{url}?{params}"
        while True:
            try:
                async for response in self.get(url=url, header=header):
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

    async def list_users(self, include_groups=False):
        header = None
        params = {
            "$filter": "accountEnabled eq true",
            "$select": "userPrincipalName,mail,transitiveMemberOf,id,createdDateTime",
        }
        # This condition is executed only during access control sync where connector will fetch all the users (licensed and unlicensed).
        if include_groups:
            params["$expand"] = "transitiveMemberOf($select=id)"

        # This condition is executed during content sync where connector fetches only licensed accounts as unlicensed users won't have any files.
        else:
            params["$filter"] += " and assignedLicenses/$count ne 0&$count=true"
            header = {"ConsistencyLevel": "eventual"}
        url = parse.urljoin(BASE_URL, ENDPOINTS[USERS])

        async for response in self.paginated_api_call(url, params, header=header):
            for user_detail in response:
                yield user_detail

    async def list_groups(self, user_id):
        url = parse.urljoin(BASE_URL, ENDPOINTS[GROUPS].format(user_id=user_id))
        async for response in self.paginated_api_call(url):
            for group_detail in response:
                yield group_detail

    async def list_file_permission(self, user_id, file_id):
        url = parse.urljoin(
            BASE_URL, ENDPOINTS[PERMISSIONS].format(user_id=user_id, item_id=file_id)
        )
        async for response in self.paginated_api_call(url):
            for permission_detail in response:
                yield permission_detail

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
    dls_enabled = True
    incremental_sync_enabled = True

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
            "use_document_level_security": {
                "display": "toggle",
                "label": "Enable document level security",
                "order": 6,
                "tooltip": "Document level security ensures identities and permissions set in OneDrive are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.",
                "type": "bool",
                "value": False,
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 7,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "ui_restrictions": ["advanced"],
                "value": False,
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

        file_size = int(file["size"])
        if not (doit and file_size > 0):
            return

        filename = file["title"]

        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": file["_id"],
            "_timestamp": file["_timestamp"],
        }
        self._logger.debug(f"Downloading file content from url: {download_url}")
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(self.client.get, url=download_url),
            ),
        )

    def prepare_doc(self, file):
        modified_document = {
            "type": FILE if file.get(FILE) else FOLDER,
            "title": file.get("name"),
            "_id": file.get("id"),
            "_timestamp": file.get("lastModifiedDateTime"),
            "created_at": file.get("createdDateTime"),
            "size": file.get("size"),
            "url": file.get("webUrl"),
        }
        if self._dls_enabled():
            modified_document[ACCESS_CONTROL] = file[ACCESS_CONTROL]
        return modified_document

    def _dls_enabled(self):
        if self._features is None:
            return False

        if not self._features.document_level_security_enabled():
            return False

        return self.configuration["use_document_level_security"]

    async def _decorate_with_access_control(self, document, user_id):
        if self._dls_enabled():
            entity_permissions = await self.get_entity_permission(
                user_id=user_id, file_id=document.get("id")
            )
            document[ACCESS_CONTROL] = list(
                set(document.get(ACCESS_CONTROL, []) + entity_permissions)
            )
        return document

    async def _user_access_control_doc(self, user):
        self._logger.info("Fetching users for access control sync")
        email = user.get("mail")
        username = user.get("userPrincipalName")

        prefixed_email = _prefix_email(email)
        prefixed_username = _prefix_user(username)
        prefixed_user_id = _prefix_user_id(user.get("id"))

        prefixed_groups = set()
        user_groups = user.get("transitiveMemberOf", [])
        if len(user_groups) < 100:  # $expand param has a max of 100
            for group in user_groups:
                prefixed_groups.add(_prefix_group(group.get("id")))
        else:
            async for group in self.client.list_groups(user_id=user.get("id")):
                prefixed_groups.add(_prefix_group(group.get("id")))

        access_control = list(
            {prefixed_email, prefixed_username, prefixed_user_id}.union(prefixed_groups)
        )
        return {
            "_id": email if email else username,
            "identity": {
                "email": prefixed_email,
                "username": prefixed_username,
                "user_id": prefixed_user_id,
            },
            "created_at": user.get("createdDateTime", iso_utc()),
        } | es_access_control_query(access_control)

    async def get_access_control(self):
        if not self._dls_enabled():
            self._logger.warning("DLS is not enabled. Skipping")
            return

        self._logger.info("Fetching all users")
        async for user in self.client.list_users(include_groups=True):
            yield await self._user_access_control_doc(user=user)

    async def get_entity_permission(self, user_id, file_id):
        if not self._dls_enabled():
            return []

        self._logger.debug(
            f"Fetching entity permissions for user_id: {user_id} and file_id: {file_id}"
        )
        permissions = []
        async for permission in self.client.list_file_permission(
            user_id=user_id, file_id=file_id
        ):
            if identity := permission.get("grantedToV2"):
                permissions.append(_prefix_user_id(identity.get("user").get("id")))

            if identities := permission.get("grantedToIdentitiesV2"):
                for identity in identities:
                    user_permission = identity.get("user", {}).get("id")
                    group_permission = identity.get("group", {}).get("id")

                    if user_permission:
                        permissions.append(_prefix_user_id(user_permission))

                    if group_permission:
                        permissions.append(_prefix_group(group_permission))

        return permissions

    def _prepare_batch(self, request_id, url):
        return {"id": str(request_id), "method": "GET", "url": url, "retry_count": "0"}

    def pop_batch_requests(self, batched_apis):
        batch = batched_apis[: min(GRAPH_API_MAX_BATCH_SIZE, len(batched_apis))]
        batched_apis[:] = batched_apis[len(batch) :]
        return batch

    def lookup_request_by_id(self, requests, response_id):
        for request in requests:
            if request.get("id") == response_id:
                return request

    async def json_batching(self, batched_apis):
        while batched_apis:
            requests = self.pop_batch_requests(batched_apis)

            batch_url = parse.urljoin(BASE_URL, ENDPOINTS[BATCH])
            batch_request = json.dumps({"requests": requests})
            batch_response = {}
            async for batch_response in self.client.post(batch_url, batch_request):
                batch_response = await batch_response.json()

            for response in batch_response.get("responses", []):
                if response.get("status", 200) == 200:
                    yield response

                    if next_url := response.get("body", {}).get("@odata.nextLink"):
                        relative_url = next_url.split(BASE_URL)[1]
                        batched_apis.append(
                            self._prepare_batch(
                                request_id=response.get("id"), url=relative_url
                            )
                        )
                elif response.get("status", 200) == 429:
                    request = (
                        self.lookup_request_by_id(requests, response.get("id")) or {}
                    )
                    request["retry_count"] = str(int(request["retry_count"]) + 1)

                    if int(request["retry_count"]) > RETRIES:
                        logger.error(
                            f"Request {request} failed after {RETRIES} retries, giving up..."
                        )
                    else:
                        batched_apis.append(request)

    def send_document_to_es(self, entity, download_url):
        entity = self.prepare_doc(entity)

        if entity["type"] == FILE and download_url:
            return entity, partial(self.get_content, entity.copy(), download_url)
        else:
            return entity, None

    async def _bounded_concurrent_tasks(
        self, items, max_concurrency, calling_func, **kwargs
    ):
        async def process_item(item, semaphore):
            async with semaphore:
                return await calling_func(item, **kwargs)

        semaphore = asyncio.Semaphore(max_concurrency)

        tasks = [process_item(item, semaphore) for item in items]

        return await asyncio.gather(*tasks)

    def build_owned_files_url(self, user):
        user_id = user.get("id")
        files_uri = f"{ENDPOINTS[DELTA].format(user_id=user_id)}?$select={ITEM_FIELDS}"

        return self._prepare_batch(request_id=user_id, url=files_uri)

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch OneDrive objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """

        if filtering and filtering.has_advanced_rules():
            advanced_rules = filtering.get_advanced_rules()
            self._logger.info(
                f"Retrieving documents from OneDrive using advanced sync rules: {advanced_rules}"
            )

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
                        yield self.send_document_to_es(entity, download_url)
        else:
            requests = []
            async for user in self.client.list_users():
                requests.append(self.build_owned_files_url(user))

            async for response in self.json_batching(batched_apis=requests):
                files = response.get("body", {}).get("value", [])
                if entities := [file for file in files if file.get("name") != "root"]:
                    if self._dls_enabled():
                        entities = await self._bounded_concurrent_tasks(
                            entities,
                            self.concurrent_downloads,
                            self._decorate_with_access_control,
                            user_id=response.get("id"),
                        )
                    for entity in entities:
                        download_url = entity.get("@microsoft.graph.downloadUrl")
                        yield self.send_document_to_es(entity, download_url)
