#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Sharepoint source module responsible to fetch documents from Sharepoint Server/Online.
"""
import asyncio
import os
import urllib.parse
from datetime import datetime, timedelta
from functools import partial
from urllib.parse import urljoin

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile

from connectors.logger import logger
from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    RetryStrategy,
    convert_to_b64,
    iso_utc,
    retryable,
)

IS_CLOUD = False
RETRY_INTERVAL = 2
DEFAULT_CONTENT_EXTRACTION = True
DEFAULT_SSL_DISABLED = True
DEFAULT_SSL_CA = ""
RETRIES = 3
FILE_SIZE_LIMIT = 10485760
CHUNK_SIZE = 1024
PING = "ping"
SITES = "sites"
LISTS = "lists"
ATTACHMENT = "attachment"
DRIVE_ITEM = "drive_item"
LIST_ITEM = "list_item"
ATTACHMENT_DATA = "attachment_data"
DOCUMENT_LIBRARY = "document_library"

URLS = {
    PING: "{host_url}/sites/{site_collections}/_api/web/webs",
    SITES: "{host_url}{parent_site_url}/_api/web/webs?$skip={skip}&$top={top}",
    LISTS: "{host_url}{parent_site_url}/_api/web/lists?$skip={skip}&$top={top}&$expand=RootFolder&$filter=(Hidden eq false)",
    ATTACHMENT: "{host_url}{value}/_api/web/GetFileByServerRelativeUrl('{file_relative_url}')/$value",
    DRIVE_ITEM: "{host_url}{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$select=Modified,Id,GUID,File,Folder&$expand=File,Folder&$top={top}",
    LIST_ITEM: "{host_url}{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$expand=AttachmentFiles&$select=*,FileRef",
    ATTACHMENT_DATA: "{host_url}{parent_site_url}/_api/web/getfilebyserverrelativeurl('{file_relative_url}')",
}
SCHEMA = {
    SITES: {
        "title": "Title",
        "url": "Url",
        "_id": "Id",
        "server_relative_url": "ServerRelativeUrl",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LISTS: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_id": "Id",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    DOCUMENT_LIBRARY: {
        "title": "Title",
        "parent_web_url": "ParentWebUrl",
        "_id": "Id",
        "_timestamp": "LastItemModifiedDate",
        "creation_time": "Created",
    },
    LIST_ITEM: {
        "title": "Title",
        "author_id": "EditorId",
        "creation_time": "Created",
        "_timestamp": "Modified",
    },
    DRIVE_ITEM: {
        "title": "Name",
        "creation_time": "TimeCreated",
        "_timestamp": "TimeLastModified",
    },
}


def encode(original_string):
    """Performs encoding on the name of objects
    containing special characters in their url, and
    replaces single quote with two single quote since quote
    is treated as an escape character

    Args:
        original_string(string): String containing special characters

    Returns:
        encoded_string(string): Parsed string without single quotes
    """
    encoded_string = urllib.parse.quote(original_string, safe="'")
    return encoded_string.replace("'", "''")


def get_expires_at(expires_in):
    """Adds seconds to the current utc time.

    Args:
        expires_in (int): Seconds after which the token will expire.
    """
    expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
    # account for clock skew
    expires_at -= timedelta(seconds=20)
    return iso_utc(when=expires_at)


def is_expired(expires_at):
    """Compares the given time with present time

    Args:
        expires_at (datetime): Time of token expires.
    """
    # Recreate in case there's no expires_at present
    if expires_at is None:
        return True
    if not isinstance(expires_at, datetime):
        expires_at = datetime.fromisoformat(expires_at)
    if datetime.utcnow() >= expires_at:
        return True
    else:
        return False


class SharepointDataSource(BaseDataSource):
    """Sharepoint"""

    name = "Sharepoint"
    service_type = "sharepoint"

    def __init__(self, configuration):
        """Setup the connection to the Sharepoint

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.is_cloud = self.configuration["is_cloud"]
        self.site_collections = self.configuration["site_collections"]
        self.ssl_disabled = self.configuration["ssl_disabled"]
        self.host_url = self.configuration["host_url"]
        self.certificate = self.configuration["ssl_ca"]
        self.enable_content_extraction = self.configuration["enable_content_extraction"]
        self.retry_count = self.configuration["retry_count"]
        self.ssl_ctx = False
        self.session = None
        self.access_token = None
        self.token_expires_in = None

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for Sharepoint

        Returns:
            dictionary: Default configuration.
        """
        return {
            "is_cloud": {
                "value": IS_CLOUD,
                "label": "True if Sharepoint Online, False if Sharepoint Server",
                "type": "bool",
            },
            "username": {
                "value": "demo_user",
                "label": "Sharepoint Server username",
                "type": "str",
            },
            "password": {
                "value": "abc@123",
                "label": "Sharepoint Server password",
                "type": "str",
            },
            "client_id": {
                "value": "client#123",
                "label": "Sharepoint Online client id",
                "type": "str",
            },
            "secret_id": {
                "value": "secret#123",
                "label": "Sharepoint Online secret id",
                "type": "str",
            },
            "tenant": {
                "value": "demo_tenant",
                "label": "Sharepoint Online tenant",
                "type": "str",
            },
            "tenant_id": {
                "value": "tenant#123",
                "label": "Sharepoint Online tenant id",
                "type": "str",
            },
            "host_url": {
                "value": "http://127.0.0.1:8491",
                "label": "Sharepoint host url",
                "type": "str",
            },
            "site_collections": {
                "value": "collection1",
                "label": "List of Sharepoint site collections to index",
                "type": "list",
            },
            "ssl_disabled": {
                "value": DEFAULT_SSL_DISABLED,
                "label": "Disable SSL verification",
                "type": "bool",
            },
            "ssl_ca": {
                "value": DEFAULT_SSL_CA,
                "label": "SSL certificate",
                "type": "str",
            },
            "enable_content_extraction": {
                "value": DEFAULT_CONTENT_EXTRACTION,
                "label": "Enable content extraction (true/false)",
                "type": "bool",
            },
            "retry_count": {
                "value": RETRIES,
                "label": "Maximum retries per request",
                "type": "int",
            },
        }

    def _validate_configuration(self):
        """Validates whether user input is empty or not for configuration fields

        Raises:
            Exception: Configured keys can't be empty.
        """
        logger.info("Validating Sharepoint Configuration...")

        connection_fields = (
            [
                "host_url",
                "site_collections",
                "client_id",
                "secret_id",
                "tenant",
                "tenant_id",
            ]
            if self.is_cloud
            else ["host_url", "site_collections", "username", "password"]
        )

        if empty_connection_fields := [
            field for field in connection_fields if self.configuration[field] == ""
        ]:
            raise Exception(
                f"Configured keys: {empty_connection_fields} can't be empty."
            )
        if not self.ssl_disabled and self.certificate == "":
            raise Exception("Configured key ssl_ca can't be empty when SSL is enabled")

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def _set_access_token(self):
        """Set access token using configuration fields"""
        if not is_expired(expires_at=self.token_expires_in):
            return
        tenant_id = self.configuration["tenant_id"]
        logger.info("Generating access token...")
        url = f"https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
        data = {
            "grant_type": "client_credentials",
            "resource": f"00000003-0000-0ff1-ce00-000000000000/{self.configuration['tenant']}.sharepoint.com@{tenant_id}",
            "client_id": f"{self.configuration['client_id']}@{tenant_id}",
            "client_secret": self.configuration["secret_id"],
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with aiohttp.request(
            method="POST", url=url, data=data, headers=headers
        ) as response:
            json_data = await response.json()
            self.access_token = json_data["access_token"]
            self.token_expires_in = get_expires_at(int(json_data["expires_in"]))

    async def _generate_session(self):
        """Generate base client session using configuration fields

        Returns:
            ClientSession: Base client session.
        """
        logger.info("Generating aiohttp Client Session...")
        request_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=None)

        if self.is_cloud:
            basic_auth = None
            await self._set_access_token()
        else:
            basic_auth = aiohttp.BasicAuth(
                login=self.configuration["username"],
                password=self.configuration["password"],
            )
        return aiohttp.ClientSession(
            auth=basic_auth,
            headers=request_headers,
            timeout=timeout,
            raise_for_status=True,
        )

    async def close(self):
        """Closes unclosed client session"""
        if self.session is None:
            return
        await self.session.close()

    async def _api_call(self, url_name, url="", **url_kwargs):
        """Make an API call to the Sharepoint Server/Online

        Args:
            url_name (str): Sharepoint url name to be executed.
            url(str, optional): Paginated url for drive and list items. Defaults to "".
            url_kwargs (dict): Url kwargs to format the query.
        Raises:
            exception: An instance of an exception class.

        Yields:
            data: API response.
        """
        retry = 0
        # If pagination happens for list and drive items then next pagination url comes in response which will be passed in url field.
        if url == "":
            url = URLS[url_name].format(**url_kwargs)
        while True:
            try:
                async with self.session.get(
                    url=url,
                    ssl=self.ssl_ctx,
                    headers={"Authorization": f"Bearer {self.access_token}"}
                    if self.is_cloud
                    else None,
                ) as result:
                    if url_name == ATTACHMENT:
                        yield result
                    else:
                        yield await result.json()
                    break
            except Exception as exception:
                if isinstance(
                    exception, aiohttp.client_exceptions.ClientResponseError
                ) and "token has expired" in exception.headers.get(
                    "x-ms-diagnostics", ""
                ):
                    await self._set_access_token()
                elif isinstance(
                    exception, aiohttp.client_exceptions.ServerDisconnectedError
                ):
                    await self.close()
                    self.session = await self._generate_session()
                retry += 1
                if retry > self.retry_count:
                    raise exception
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                await asyncio.sleep(RETRY_INTERVAL**retry)

    def format_document(
        self,
        item,
        document_type,
        item_type=None,
        file_name=None,
    ):
        """Prepare key mappings for sites, lists, list items and drive items

        Args:
            item (dictionary): Document from Sharepoint.
            document_type(string): Type of document(i.e. list_item and drive_item).
            item_type(string, optional): Type of item i.e. File or Folder. Defaults to None.
            file_name(string, optional): Name of file. Defaults to None.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": document_type}

        if document_type in [LISTS, DOCUMENT_LIBRARY]:
            document["url"] = urljoin(
                self.host_url, item["RootFolder"]["ServerRelativeUrl"]
            )

        elif document_type == DRIVE_ITEM:
            document.update(
                {
                    "_id": item["GUID"],
                    "size": item.get("File", {}).get("Length"),
                    "url": urljoin(self.host_url, item[item_type]["ServerRelativeUrl"]),
                    "type": item_type,
                }
            )

        elif document_type == LIST_ITEM:
            document.update(
                {
                    "_id": item["_id"] if "_id" in item.keys() else item["GUID"],
                    "file_name": file_name,
                    "size": item.get("size"),
                    "url": item["url"],
                }
            )

        for elasticsearch_field, sharepoint_field in SCHEMA[document_type].items():
            document[elasticsearch_field] = (
                item[item_type][sharepoint_field]
                if document_type in [DRIVE_ITEM]
                else item[sharepoint_field]
            )

        return document

    async def ping(self):
        """Verify the connection with Sharepoint"""

        self._validate_configuration()

        if not self.ssl_disabled:
            self.ssl_ctx = self._ssl_context(certificate=self.certificate)

        self.session = await self._generate_session()
        try:
            await anext(
                self._api_call(
                    url_name=PING,
                    site_collections=self.site_collections[0],
                    host_url=self.host_url,
                )
            )
            logger.info("Successfully connected to the Sharepoint")
        except Exception:
            logger.exception("Error while connecting to the Sharepoint")
            raise

    async def get_content(
        self, document, file_relative_url, site_url, timestamp=None, doit=None
    ):
        """Get content of list items and drive items

        Args:
            document (dictionary): Modified document.
            file_relative_url (str): Relative url of file
            site_url (str): Site path of sharepoint
            timestamp (timestamp, optional): Timestamp of item last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to None.

        Returns:
            dictionary: Content document with id, timestamp & text.
        """

        if not (self.enable_content_extraction and doit and document["size"]):
            return

        document_size = int(document["size"])
        if document_size > FILE_SIZE_LIMIT:
            logger.warning(
                f"File size {document_size} of file {document['title']} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        source_file_name = ""

        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            async for response in self._api_call(
                url_name=ATTACHMENT,
                host_url=self.host_url,
                value=site_url,
                file_relative_url=file_relative_url,
            ):
                async for data in response.content.iter_chunked(CHUNK_SIZE):
                    await async_buffer.write(data)
            source_file_name = async_buffer.name

        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        async with aiofiles.open(file=source_file_name, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            attachment_content = (await target_file.read()).strip()
        await remove(source_file_name)
        return {
            "_id": document.get("id"),
            "_timestamp": document.get("_timestamp"),
            "_attachment": attachment_content,
        }

    async def invoke_get_call(self, site_url, param_name, list_id=None):
        """Invokes a GET call to the Sharepoint Server/Online.

        Args:
            site_url(string): site url to the sharepoint farm.
            param_name(string): parameter name whether it is SITES, LISTS, DRIVE_ITEM, LIST_ITEM.
            list_id(string, optional): Id of list item or drive item. Defaults to None.
        Yields:
            Response of the GET call.
        """
        skip, top = 0, 5000
        next_url = ""
        while True:
            if param_name in [SITES, LISTS]:
                response = await anext(
                    self._api_call(
                        url_name=param_name,
                        parent_site_url=site_url,
                        skip=skip,
                        top=top,
                        host_url=self.host_url,
                    )
                )
                response_result = response.get("value", [])
                yield response_result

                skip += 5000
                if len(response_result) < 5000:
                    break
            elif param_name in [
                DRIVE_ITEM,
                LIST_ITEM,
            ]:
                if next_url != "":
                    response = await anext(
                        self._api_call(
                            url_name=param_name,
                            url=next_url,
                        )
                    )
                else:
                    response = await anext(
                        self._api_call(
                            url_name=param_name,
                            parent_site_url=site_url,
                            list_id=list_id,
                            top=top,
                            host_url=self.host_url,
                        )
                    )
                response_result = response.get("value", [])
                yield response_result

                next_url = response.get("odata.nextLink", "")
                if next_url == "":
                    break

    async def get_sites(self, site_url):
        """Get sites from Sharepoint Server/Online

        Args:
            site_url(string): Parent site relative path.
        Yields:
            site_server_url(string): Site path.
        """
        async for sites_data in self.invoke_get_call(
            site_url=site_url, param_name=SITES
        ):
            for data in sites_data:
                async for sub_site in self.get_sites(
                    site_url=data["ServerRelativeUrl"]
                ):
                    yield sub_site
                yield self.format_document(item=data, document_type=SITES)

    async def get_list_items(self, list_id, site_url, server_relative_url):
        """This method fetches items from all the lists in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
        Yields:
            dictionary: dictionary containing meta-data of the list item.
        """
        file_relative_url = None
        async for list_items_data in self.invoke_get_call(
            site_url=site_url, param_name=LIST_ITEM, list_id=list_id
        ):
            for result in list_items_data:
                if not result.get("Attachments"):
                    url = f"{self.host_url}{server_relative_url}/DispForm.aspx?ID={result['Id']}&Source={self.host_url}{server_relative_url}/AllItems.aspx&ContentTypeId={result['ContentTypeId']}"
                    result["url"] = url
                    yield self.format_document(
                        item=result,
                        document_type=LIST_ITEM,
                    ), file_relative_url
                    continue

                for attachment_file in result.get("AttachmentFiles"):
                    file_relative_url = encode(
                        original_string=attachment_file.get("ServerRelativeUrl")
                    )

                    attachment_data = await anext(
                        self._api_call(
                            url_name=ATTACHMENT_DATA,
                            host_url=self.host_url,
                            parent_site_url=site_url,
                            file_relative_url=file_relative_url,
                        )
                    )
                    result["size"] = attachment_data.get("Length")
                    result["_id"] = attachment_data["UniqueId"]
                    result["url"] = urljoin(
                        self.host_url, attachment_file.get("ServerRelativeUrl")
                    )

                    if (
                        os.path.splitext(attachment_file["FileName"])[-1]
                        not in TIKA_SUPPORTED_FILETYPES
                    ):
                        file_relative_url = None

                    yield self.format_document(
                        item=result,
                        document_type=LIST_ITEM,
                        file_name=attachment_file.get("FileName"),
                    ), file_relative_url

    async def get_drive_items(self, list_id, site_url, server_relative_url):
        """This method fetches items from all the drives in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
        Yields:
            dictionary: dictionary containing meta-data of the drive item.
        """
        async for drive_items_data in self.invoke_get_call(
            site_url=site_url, param_name=DRIVE_ITEM, list_id=list_id
        ):
            for result in drive_items_data:
                file_relative_url = None
                item_type = "Folder"

                if result.get("File", {}).get("TimeLastModified"):
                    item_type = "File"
                    file_relative_url = (
                        encode(original_string=result["File"]["ServerRelativeUrl"])
                        if os.path.splitext(result["File"]["Name"])[-1]
                        in TIKA_SUPPORTED_FILETYPES
                        else None
                    )

                yield self.format_document(
                    item=result,
                    document_type=DRIVE_ITEM,
                    item_type=item_type,
                ), file_relative_url

    async def get_lists_and_items(self, site):
        """Executes the logic to fetch lists and items (list-item, drive-item) in async manner.

        Args:
            site(string): Path of site.
        Yields:
            dictionary: dictionary containing meta-data of the list, list item and drive item.
        """
        async for list_data in self.invoke_get_call(site_url=site, param_name=LISTS):
            for result in list_data:
                if result.get("BaseType") == 1:
                    server_url = None
                    document_type = DOCUMENT_LIBRARY
                    func = self.get_drive_items
                else:
                    document_type = LISTS
                    server_url = result["RootFolder"]["ServerRelativeUrl"]
                    func = self.get_list_items

                yield self.format_document(
                    item=result, document_type=document_type
                ), None

                async for item, file_relative_url in func(
                    list_id=result.get("Id"),
                    site_url=result.get("ParentWebUrl"),
                    server_relative_url=server_url,
                ):
                    yield item, file_relative_url

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch Sharepoint objects in an async manner.

        Yields:
            dictionary: dictionary containing meta-data of the Sharepoint objects.
        """
        for collection in self.site_collections:
            async for site_document in self.get_sites(site_url=f"/sites/{collection}"):
                yield site_document, None
                site_url = site_document["server_relative_url"]
                async for item, file_relative_url in self.get_lists_and_items(
                    site=site_url
                ):
                    if file_relative_url is None:
                        yield item, None
                    else:
                        yield item, partial(
                            self.get_content, item, file_relative_url, site_url
                        )
