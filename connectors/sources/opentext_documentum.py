#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Opentext Documentum source module responsible to fetch documents from Opentext Documentum.
"""
from copy import copy
from functools import cached_property, partial
from urllib import parse

import aiohttp
from aiohttp.client_exceptions import ClientResponseError, ServerConnectionError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    retryable,
    ssl_context,
)

RETRIES = 3
RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30

ITEMS_PER_PAGE = 100
MAX_USER_FETCH_LIMIT = 1000

WILDCARD = "*"
PING = "ping"
REPOSITORIES = "repositories"
REPOSITORY_BY_NAME = "repository_by_name"
CABINETS = "cabinets"
FOLDERS = "folders"
RECURSIVE_FOLDER = "recursive-folder"
FILES = "files"
URLS = {
    PING: "/dctm-rest/repositories?items-per-page=1",
    REPOSITORIES: "/dctm-rest/repositories?page={page}&items-per-page={items_per_page}",
    REPOSITORY_BY_NAME: "/dctm-rest/repositories/{repository_name}",
    CABINETS: "/dctm-rest/repositories/{repository_name}/cabinets?page={page}&items-per-page={items_per_page}",
    FOLDERS: "/dctm-rest/repositories/{repository_name}/folders?page={page}&items-per-page={items_per_page}",
    RECURSIVE_FOLDER: "/dctm-rest/repositories/{repository_name}/folders/{folder_id}/folders?page={page}&items-per-page={items_per_page}",
    FILES: "/dctm-rest/repositories/{repository_name}/folders/{folder_id}/documents?page={page}&items-per-page={items_per_page}",
}


class ThrottledError(Exception):
    """Internal exception class to indicate that request was throttled by the API"""

    pass


class InternalServerError(Exception):
    pass


class NotFound(Exception):
    pass


class OpentextDocumentumClient:
    """Opentext Documentum client to handle API calls made to Opentext Documentum"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.host_url = self.configuration["host_url"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.certificate = self.configuration["ssl_ca"]

        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def _get_session(self):
        """Generate and return base client session with configuration fields

        Returns:
            aiohttp.ClientSession: An instance of Client Session
        """
        self._logger.debug("Creating a client session")
        login, password = (
            self.configuration["username"],
            self.configuration["password"],
        )

        auth = aiohttp.BasicAuth(login=login, password=password)
        timeout = aiohttp.ClientTimeout(total=None)
        return aiohttp.ClientSession(
            auth=auth,
            headers={
                "content-type": "application/vnd.emc.documentum+json",
            },
            timeout=timeout,
            raise_for_status=True,
        )

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        await self._get_session.close()
        del self._get_session

    async def _handle_client_errors(self, url, exception):
        if exception.status == 429:
            response_headers = exception.headers or {}
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
                    f"Rate Limited but Retry-After header wasn't found, using default retry time: {DEFAULT_RETRY_SECONDS} seconds"
                )
            self._logger.debug(f"Rate Limit reached: retry in {retry_seconds} seconds")

            await self._sleeps.sleep(retry_seconds)
            raise ThrottledError
        elif exception.status == 404:
            self._logger.exception(f"Getting Not Found Error for url: {url}")
            raise NotFound
        elif exception.status == 500:
            self._logger.exception("Internal Server Error occurred")
            raise InternalServerError
        else:
            raise

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        skipped_exceptions=NotFound,
    )
    async def api_call(self, url_name=None, **url_kwargs):
        """Make a GET call for the Opentext Documentum API using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit
            url_kwargs (dict): Url kwargs to format the query.

        Raises:
            exception: An instance of an exception class.

        Yields:
            response: Return api response.
        """
        url = url_kwargs.get("url") or parse.urljoin(
            self.host_url, URLS[url_name].format(**url_kwargs)  # pyright: ignore
        )
        self._logger.debug(f"Making a GET call for url: {url}")
        while True:
            try:
                async with self._get_session.get(
                    url=url,
                    ssl=self.ssl_ctx,
                ) as response:
                    yield response
                    break
            except ServerConnectionError:
                self._logger.exception(f"Getting ServerConnectionError for url: {url}")
                await self.close_session()
                raise
            except ClientResponseError as exception:
                await self._handle_client_errors(url=url, exception=exception)

    async def paginated_api_call(self, url_name, **kwargs):
        """Make a paginated API call for Opentext Documentum objects using the passed url_name with retry for the failed API calls.

        Args:
            url_name (str): URL Name to identify the API endpoint to hit

        Yields:
            response: Return api response.
        """
        page = 0
        self._logger.info(
            f"Started pagination for API url: {parse.urljoin(self.host_url, URLS[url_name].format(page=page, items_per_page=ITEMS_PER_PAGE, **kwargs))} with {ITEMS_PER_PAGE} items per page"
        )

        while True:
            try:
                async for response in self.api_call(
                    url_name=url_name,
                    page=page,
                    items_per_page=ITEMS_PER_PAGE,
                    repository_name=kwargs.get("repository_name"),
                    folder_id=kwargs.get("folder_id"),
                ):
                    response_json = await response.json()
                    if not response_json.get("entries"):
                        return
                    yield response_json
                    page += 1
            except Exception as exception:
                self._logger.warning(
                    f"Skipping data for type: {url_name}, page: {page}. Error: {exception}."
                )
                break


class OpentextDocumentumDataSource(BaseDataSource):
    """Opentext Documentum"""

    name = "Opentext Documentum"
    service_type = "opentext_documentum"

    def __init__(self, configuration):
        """Setup the connection to the Opentext Documentum

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
            logger_ (DocumentLogger): Object of DocumentLogger class.
        """
        super().__init__(configuration=configuration)
        self.opentext_client = OpentextDocumentumClient(configuration=configuration)
        self.repositories = self.configuration["repositories"]

    def _set_internal_logger(self):
        self.opentext_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Opentext Documentum

        Returns:
            dictionary: Default configuration.
        """
        return {
            "host_url": {
                "label": "Opentext Documentum host url",
                "order": 1,
                "type": "str",
            },
            "username": {
                "label": "Username",
                "order": 2,
                "type": "str",
            },
            "password": {
                "label": "Password",
                "sensitive": True,
                "order": 3,
                "type": "str",
            },
            "repositories": {
                "display": "textarea",
                "label": "Repositories",
                "order": 4,
                "tooltip": "This configurable field is ignored when Advanced Sync Rules are used.",
                "type": "list",
            },
            "ssl_enabled": {
                "display": "toggle",
                "label": "Enable SSL",
                "order": 5,
                "type": "bool",
                "value": False,
            },
            "ssl_ca": {
                "depends_on": [{"field": "ssl_enabled", "value": True}],
                "label": "SSL certificate",
                "order": 6,
                "type": "str",
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

    async def close(self):
        """Closes unclosed client session"""
        await self.opentext_client.close_session()

    async def get_content(self, attachment, timestamp=None, doit=False):
        """Extracts the content for allowed file types.

        Args:
            attachment (dictionary): Formatted attachment document.
            timestamp (timestamp, optional): Timestamp of attachment last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with _id, _timestamp and attachment content
        """
        if not doit:
            return

        file_size = int(attachment["size"])
        if file_size <= 0:
            return

        filename = attachment["title"]
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(
            file_extension,
            filename,
            file_size,
        ):
            return

        download_url = attachment["content"]["src"]

        self._logger.debug(
            f"Downloading content for file: {filename} with download url: {download_url}"
        )

        document = {
            "_id": attachment["id"],
            "_timestamp": attachment["updated"],
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(self.opentext_client.api_call, url=download_url),
            ),
        )

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields.
        Also validate, if user configured spaces are available in Confluence.

        Raises:
            Exception: Configured keys can't be empty
        """
        await super().validate_config()
        await self._remote_validation()

    async def _remote_validation(self):
        if self.repositories == [WILDCARD]:
            return
        available_repos = []
        async for response in self.opentext_client.paginated_api_call(
            url_name=REPOSITORIES
        ):
            repositories = response.get("entries", [])
            available_repos.extend(
                [repository.get("title") for repository in repositories]
            )
        if unavailable_repos := set(self.repositories) - set(available_repos):
            msg = f"Repositories '{', '.join(unavailable_repos)}' are not available. Available repositories are: '{', '.join(available_repos)}'"
            raise ConfigurableFieldValueError(msg)

    async def ping(self):
        """Verify the connection with Opentext Documentum"""
        try:
            await anext(self.opentext_client.api_call(url_name=PING))
            self._logger.info("Successfully connected to Opentext Documentum")
        except Exception:
            self._logger.exception("Error while connecting to Opentext Documentum")
            raise

    async def fetch_repositories(self):
        """Reference for JSON representation of API response: https://www.dbi-services.com/blog/documentum-rest-api/"""
        if self.repositories == [WILDCARD]:
            self._logger.info(
                f"Fetching all repositories as the configuration field `repositories` is set to '{WILDCARD}'"
            )
            async for page in self.opentext_client.paginated_api_call(
                url_name=REPOSITORIES,
            ):
                for repository in page["entries"]:
                    yield {
                        "_id": repository.get("id"),
                        "_timestamp": repository.get("updated"),
                        "type": "Repository",
                        "repository_name": repository.get("title"),
                        "summary": repository.get("summary"),
                        "authors": page["author"],
                    }
        else:
            self._logger.info(
                f"Fetching data for configured repositories: {self.repositories}"
            )
            for repository_name in self.repositories:
                async for repository in self.opentext_client.paginated_api_call(
                    url_name=REPOSITORY_BY_NAME, repository_name=repository_name
                ):
                    # Assuming that we don't get `entries` field for fetching single repository by its name
                    yield {
                        "_id": repository.get("id"),
                        "_timestamp": repository.get("updated"),
                        "type": "Repository",
                        "repository_name": repository.get("title"),
                        "summary": repository.get("summary"),
                    }

    async def fetch_cabinets(self, repository_name):
        self._logger.info(f"Fetching cabinets for '{repository_name}' repository")
        async for page in self.opentext_client.paginated_api_call(
            url_name=CABINETS, repository_name=repository_name
        ):
            for cabinet in page["entries"]:
                yield {
                    "_id": cabinet.get("id"),
                    "_timestamp": cabinet.get("updated"),
                    "type": "Cabinet",
                    "cabinet_name": cabinet.get("title"),
                    "definition": cabinet.get("definition"),
                    "authors": page["author"],
                }

    async def fetch_folders_recursively(self, repository_name):
        self._logger.info(f"Fetching folders from '{repository_name}' repository")
        async for response in self.opentext_client.paginated_api_call(
            url_name=FOLDERS, repository_name=repository_name
        ):
            for folder in response["entries"]:
                yield {
                    "_id": folder.get("id"),
                    "_timestamp": folder.get("updated"),
                    "type": "Folder",
                    "title": folder.get("title"),
                }

                async for response in self.opentext_client.paginated_api_call(
                    url_name=RECURSIVE_FOLDER,
                    repository_name=repository_name,
                    folder_id=folder.get("id"),
                ):
                    for folder in response["entries"]:
                        yield {
                            "_id": folder.get("id"),
                            "_timestamp": folder.get("updated"),
                            "type": "Folder",
                            "title": folder.get("title"),
                        }

    async def fetch_files(self, repository_name, folder_id):
        self._logger.info(
            f"Fetching files for '{folder_id}' folder ID from '{repository_name}' repository"
        )
        async for response in self.opentext_client.paginated_api_call(
            url_name=FILES, repository_name=repository_name, folder_id=folder_id
        ):
            for file in response["entries"]:
                yield {
                    "_id": file.get("id"),
                    "_timestamp": file.get("updated"),
                    "type": "File",
                    "title": file.get("title"),
                    "size": file.get("size"),
                }, file

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Opentext Documentum objects in async manner

        Args:
            filtering (Filtering): Object of class Filtering

        Yields:
            dictionary: dictionary containing meta-data of the files.
        """
        async for repository in self.fetch_repositories():
            yield repository, None

            repository_name = repository["repository_name"]

            async for cabinet in self.fetch_cabinets(repository_name=repository_name):
                yield cabinet, None

            async for folder in self.fetch_folders_recursively(
                repository_name=repository_name
            ):
                async for file, response in self.fetch_files(
                    repository_name=repository_name, folder_id=folder["_id"]
                ):
                    yield file, partial(self.get_content, attachment=copy(response))

                yield folder, None
