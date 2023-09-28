#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""SharePoint source module responsible to fetch documents from SharePoint Server.
"""
import asyncio
import os
from functools import partial
from urllib.parse import quote

import aiofiles
import aiohttp
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile
from aiohttp.client_exceptions import ServerDisconnectedError

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    CancellableSleeps,
    RetryStrategy,
    convert_to_b64,
    retryable,
    ssl_context,
)

RETRY_INTERVAL = 2
DEFAULT_RETRY_SECONDS = 30
RETRIES = 3
FILE_SIZE_LIMIT = 10485760
CHUNK_SIZE = 1024
TOP = 5000
PING = "ping"
SITES = "sites"
LISTS = "lists"
ATTACHMENT = "attachment"
DRIVE_ITEM = "drive_item"
LIST_ITEM = "list_item"
ATTACHMENT_DATA = "attachment_data"
DOCUMENT_LIBRARY = "document_library"
SELECTED_FIELDS = "WikiField, Modified,Id,GUID,File,Folder"

URLS = {
    PING: "{host_url}/sites/{site_collections}/_api/web/webs",
    SITES: "{host_url}{parent_site_url}/_api/web/webs?$skip={skip}&$top={top}",
    LISTS: "{host_url}{parent_site_url}/_api/web/lists?$skip={skip}&$top={top}&$expand=RootFolder&$filter=(Hidden eq false)",
    ATTACHMENT: "{host_url}{value}/_api/web/GetFileByServerRelativeUrl('{file_relative_url}')/$value",
    DRIVE_ITEM: "{host_url}{parent_site_url}/_api/web/lists(guid'{list_id}')/items?$select={selected_field}&$expand=File,Folder&$top={top}",
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
        "author_id": "AuthorId",
        "editor_id": "EditorId",
        "creation_time": "Created",
        "_timestamp": "Modified",
    },
    DRIVE_ITEM: {
        "title": "Name",
        "creation_time": "TimeCreated",
        "_timestamp": "TimeLastModified",
    },
}


class SharepointServerClient:
    """SharePoint client to handle API calls made to SharePoint"""

    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.host_url = self.configuration["host_url"]
        self.certificate = self.configuration["ssl_ca"]
        self.ssl_enabled = self.configuration["ssl_enabled"]
        self.retry_count = self.configuration["retry_count"]
        self.site_collections = self.configuration["site_collections"]

        self.session = None
        if self.ssl_enabled and self.certificate:
            self.ssl_ctx = ssl_context(certificate=self.certificate)
        else:
            self.ssl_ctx = False

    def set_logger(self, logger_):
        self._logger = logger_

    def _get_session(self):
        """Generate base client session using configuration fields

        Returns:
            ClientSession: Base client session.
        """
        if self.session:
            return self.session
        self._logger.info("Generating aiohttp Client Session...")
        request_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        timeout = aiohttp.ClientTimeout(total=None)  # pyright: ignore

        self.session = aiohttp.ClientSession(
            auth=aiohttp.BasicAuth(
                login=self.configuration["username"],
                password=self.configuration["password"],
            ),  # pyright: ignore
            headers=request_headers,
            timeout=timeout,
            raise_for_status=True,
        )
        return self.session

    def format_url(
        self,
        relative_url,
        list_item_id=None,
        content_type_id=None,
        is_list_item_has_attachment=False,
    ):
        if is_list_item_has_attachment:
            return (
                self.host_url
                + quote(relative_url)
                + "/DispForm.aspx?ID="
                + list_item_id
                + "&Source="
                + self.host_url
                + quote(relative_url)
                + "/AllItems.aspx&ContentTypeId="
                + content_type_id
            )
        else:
            return self.host_url + quote(relative_url)

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.close()  # pyright: ignore
        self.session = None

    async def convert_file_to_b64(self, source_file_name):
        """This method converts the file content into b64
        Args:
            source_file_name: Name of source file
        Returns:
            attachment_content: Attachment content in b64
        """
        async with aiofiles.open(file=source_file_name) as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            attachment_content = (await target_file.read()).strip()
        try:
            await remove(source_file_name)  # pyright: ignore
        except Exception as exception:
            self._logger.warning(
                f"Could not remove file: {source_file_name}. Error: {exception}"
            )
        return attachment_content

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def get_site_pages_content(
        self, document, list_response, timestamp=None, doit=False
    ):
        """Get content of site pages for SharePoint

        Args:
            document (dictionary): Modified document.
            list_response (dict): Dictionary of list item response
            timestamp (timestamp, optional): Timestamp of item last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with id, timestamp & text.
        """
        document_size = int(document["size"])
        if not (doit and document_size):
            return

        filename = (
            document["title"] if document["type"] == "File" else document["file_name"]
        )

        if document_size > FILE_SIZE_LIMIT:
            self._logger.warning(
                f"File size {document_size} of file {filename} is larger than {FILE_SIZE_LIMIT} bytes. Discarding file content"
            )
            return

        source_file_name = ""

        response_data = list_response["WikiField"]

        if response_data is None:
            return

        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            await async_buffer.write(bytes(response_data, "utf-8"))

            source_file_name = async_buffer.name

        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        return {
            "_id": document.get("id"),
            "_timestamp": document.get("_timestamp"),
            "_attachment": await self.convert_file_to_b64(source_file_name),
        }

    async def _api_call(self, url_name, url="", **url_kwargs):
        """Make an API call to the SharePoint Server

        Args:
            url_name (str): SharePoint url name to be executed.
            url(str, optional): Paginated url for drive and list items. Defaults to "".
            url_kwargs (dict): Url kwargs to format the query.
        Raises:
            exception: An instance of an exception class.

        Yields:
            data: API response.
        """
        retry = 1
        # If pagination happens for list and drive items then next pagination url comes in response which will be passed in url field.
        if url == "":
            url = URLS[url_name].format(**url_kwargs)

        headers = None

        while True:
            try:
                async with self._get_session().get(  # pyright: ignore
                    url=url,
                    ssl=self.ssl_ctx,  # pyright: ignore
                    headers=headers,
                ) as result:
                    if url_name == ATTACHMENT:
                        yield result
                    else:
                        yield await result.json()
                    break
            except Exception as exception:
                if isinstance(
                    exception,
                    ServerDisconnectedError,
                ):
                    await self.close_session()
                if retry > self.retry_count:
                    break
                logger.warning(
                    f"Retry count: {retry} out of {self.retry_count}. Exception: {exception}"
                )
                retry += 1

                await self._sleeps.sleep(RETRY_INTERVAL**retry)

    async def _fetch_data_with_next_url(
        self, site_url, list_id, param_name, selected_field=""
    ):
        """Invokes a GET call to the SharePoint Server for calling list and drive item API.

        Args:
            site_url(string): site url to the SharePoint farm.
            list_id(string): Id of list item or drive item.
            param_name(string): parameter name whether it is DRIVE_ITEM, LIST_ITEM.
            selected_field(string): Select query parameter for drive item.
        Yields:
            Response of the GET call.
        """
        next_url = ""
        while True:
            if next_url != "":
                async for response in self._api_call(
                    url_name=param_name,
                    url=next_url,
                ):
                    yield response.get("value", [])  # pyright: ignore

                    next_url = response.get("odata.nextLink", "")
            else:
                async for response in self._api_call(
                    url_name=param_name,
                    parent_site_url=site_url,
                    list_id=list_id,
                    top=TOP,
                    host_url=self.host_url,
                    selected_field=selected_field,
                ):
                    yield response.get("value", [])  # pyright: ignore

                    next_url = response.get("odata.nextLink", "")
            if next_url == "":
                break

    async def _fetch_data_with_query(self, site_url, param_name):
        """Invokes a GET call to the SharePoint Server for calling site and list API.

        Args:
            site_url(string): site url to the SharePoint farm.
            param_name(string): parameter name whether it is SITES, LISTS.
        Yields:
            Response of the GET call.
        """
        skip = 0
        response_result = []
        while True:
            async for response in self._api_call(
                url_name=param_name,
                parent_site_url=site_url,
                skip=skip,
                top=TOP,
                host_url=self.host_url,
            ):
                response_result = response.get("value", [])  # pyright: ignore
                yield response_result

                skip += TOP
            if len(response_result) < TOP:
                break

    async def get_sites(self, site_url):
        """Get sites from SharePoint Server

        Args:
            site_url(string): Parent site relative path.
        Yields:
            site_server_url(string): Site path.
        """
        async for sites_data in self._fetch_data_with_query(
            site_url=site_url, param_name=SITES
        ):
            for data in sites_data:
                async for sub_site in self.get_sites(  # pyright: ignore
                    site_url=data["ServerRelativeUrl"]
                ):
                    yield sub_site
                yield data

    async def get_lists(self, site_url):
        """Get site lists from SharePoint Server

        Args:
            site_url(string): Parent site relative path.
        Yields:
            list_data(string): Response of list API call
        """
        async for list_data in self._fetch_data_with_query(
            site_url=site_url, param_name=LISTS
        ):
            yield list_data

    async def get_attachment(self, site_url, file_relative_url):
        """Execute the call for fetching attachment metadata

        Args:
            site_url(string): Parent site relative path
            file_relative_url(string): Relative url of file
        Returns:
            attachment_data(dictionary): Attachment metadata
        """
        return await anext(
            self._api_call(
                url_name=ATTACHMENT_DATA,
                host_url=self.host_url,
                parent_site_url=site_url,
                file_relative_url=file_relative_url,
            )
        )

    def verify_filename_for_extraction(self, filename, relative_url):
        attachment_extension = list(os.path.splitext(filename))
        if "" in attachment_extension:
            attachment_extension.remove("")
        if "." not in filename:
            self._logger.warning(
                f"Files without extension are not supported by TIKA, skipping {filename}."
            )
            return
        if attachment_extension[-1].lower() not in TIKA_SUPPORTED_FILETYPES:
            return
        return relative_url

    async def get_list_items(self, list_id, site_url, server_relative_url, **kwargs):
        """This method fetches items from all the lists in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
        Yields:
            dictionary: dictionary containing meta-data of the list item.
        """
        file_relative_url = None
        async for list_items_data in self._fetch_data_with_next_url(
            site_url=site_url, list_id=list_id, param_name=LIST_ITEM
        ):
            for result in list_items_data:
                if not result.get("Attachments"):
                    url = self.format_url(
                        relative_url=server_relative_url,
                        list_item_id=str(result["Id"]),
                        content_type_id=result["ContentTypeId"],
                        is_list_item_has_attachment=True,
                    )
                    result["url"] = url
                    yield result, file_relative_url
                    continue

                for attachment_file in result.get("AttachmentFiles"):
                    file_relative_url = quote(
                        attachment_file.get("ServerRelativeUrl").replace(
                            "%27", "%27%27"
                        )
                    )

                    attachment_data = await self.get_attachment(
                        site_url, file_relative_url
                    )
                    result["Length"] = attachment_data.get("Length")  # pyright: ignore
                    result["_id"] = attachment_data["UniqueId"]  # pyright: ignore
                    result["url"] = self.format_url(
                        relative_url=attachment_file.get("ServerRelativeUrl")
                    )
                    result["file_name"] = attachment_file.get("FileName")
                    result["server_relative_url"] = attachment_file["ServerRelativeUrl"]

                    file_relative_url = self.verify_filename_for_extraction(
                        filename=attachment_file["FileName"],
                        relative_url=file_relative_url,
                    )

                    yield result, file_relative_url

    async def get_drive_items(self, list_id, site_url, server_relative_url, **kwargs):
        """This method fetches items from all the drives in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site
            kwargs(string): Select query parameter for drive item.
        Yields:
            dictionary: dictionary containing meta-data of the drive item.
        """
        async for drive_items_data in self._fetch_data_with_next_url(
            site_url=site_url,
            list_id=list_id,
            selected_field=kwargs["selected_field"],
            param_name=DRIVE_ITEM,
        ):
            for result in drive_items_data:
                file_relative_url = None
                item_type = "Folder"

                if result.get("File", {}).get("TimeLastModified"):
                    item_type = "File"
                    filename = result["File"]["Name"]
                    file_relative_url = quote(
                        result["File"]["ServerRelativeUrl"]
                    ).replace("%27", "%27%27")
                    file_relative_url = self.verify_filename_for_extraction(
                        filename=filename, relative_url=file_relative_url
                    )
                    result["Length"] = result[item_type]["Length"]
                result["item_type"] = item_type

                yield result, file_relative_url

    async def ping(self):
        """Executes the ping call in async manner"""
        await anext(
            self._api_call(
                url_name=PING,
                site_collections=self.site_collections[0],
                host_url=self.host_url,
            )
        )


class SharepointServerDataSource(BaseDataSource):
    """SharePoint Server"""

    name = "SharePoint Server"
    service_type = "sharepoint_server"

    def __init__(self, configuration):
        """Setup the connection to the SharePoint

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.sharepoint_client = SharepointServerClient(configuration=configuration)
        self.invalid_collections = []

    def _set_internal_logger(self):
        self.sharepoint_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for SharePoint

        Returns:
            dictionary: Default configuration.
        """
        return {
            "username": {
                "label": "SharePoint Server username",
                "order": 1,
                "type": "str",
            },
            "password": {
                "label": "SharePoint Server password",
                "sensitive": True,
                "order": 2,
                "type": "str",
            },
            "host_url": {
                "label": "SharePoint host",
                "order": 3,
                "type": "str",
            },
            "site_collections": {
                "display": "textarea",
                "label": "Comma-separated list of SharePoint site collections to index",
                "order": 4,
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
            "retry_count": {
                "default_value": RETRIES,
                "display": "numeric",
                "label": "Retries per request",
                "order": 7,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
            "use_text_extraction_service": {
                "display": "toggle",
                "label": "Use text extraction service",
                "order": 8,
                "tooltip": "Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.",
                "type": "bool",
                "value": False,
            },
        }

    async def close(self):
        """Closes unclosed client session"""
        await self.sharepoint_client.close_session()

    async def _remote_validation(self):
        """Validate configured collections
        Raises:
            ConfigurableFieldValueError: Unavailable services error.
        """

        for collection in self.sharepoint_client.site_collections:
            is_invalid = True
            async for _ in self.sharepoint_client._api_call(
                url_name=PING,
                site_collections=collection,
                host_url=self.sharepoint_client.host_url,
            ):
                is_invalid = False
            if is_invalid:
                self.invalid_collections.append(collection)
        if self.invalid_collections:
            raise ConfigurableFieldValueError(
                f"Collections {', '.join(self.invalid_collections)} are not available."
            )

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured collections are available in SharePoint."""
        await super().validate_config()
        await self._remote_validation()

    async def ping(self):
        """Verify the connection with SharePoint"""
        try:
            await self.sharepoint_client.ping()
            self._logger.debug(
                f"Successfully connected to the SharePoint via {self.sharepoint_client.host_url}"
            )
        except Exception:
            self._logger.exception(
                f"Error while connecting to the SharePoint via {self.sharepoint_client.host_url}"
            )
            raise

    def map_document_with_schema(
        self,
        document,
        item,
        document_type,
    ):
        """Prepare key mappings for documents

        Args:
            document(dictionary): Modified document
            item (dictionary): Document from SharePoint.
            document_type(string): Type of document(i.e. site,list,list_iitem, drive_item and document_library).

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        for elasticsearch_field, sharepoint_field in SCHEMA[document_type].items():
            document[elasticsearch_field] = item[sharepoint_field]

    def format_lists(
        self,
        item,
        document_type,
    ):
        """Prepare key mappings for list

        Args:
            item (dictionary): Document from SharePoint.
            document_type(string): Type of document(i.e. list and document_library).

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": document_type}

        document["url"] = self.sharepoint_client.format_url(
            relative_url=item["RootFolder"]["ServerRelativeUrl"]
        )
        document["server_relative_url"] = item["RootFolder"]["ServerRelativeUrl"]

        self.map_document_with_schema(
            document=document, item=item, document_type=document_type
        )
        return document

    def format_sites(self, item):
        """Prepare key mappings for site

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": SITES}

        self.map_document_with_schema(document=document, item=item, document_type=SITES)
        return document

    def format_drive_item(
        self,
        item,
    ):
        """Prepare key mappings for drive items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": DRIVE_ITEM}
        item_type = item["item_type"]

        document.update(
            {  # pyright: ignore
                "_id": item["GUID"],
                "size": int(item.get("File", {}).get("Length", 0)),
                "url": self.sharepoint_client.format_url(
                    relative_url=item[item_type]["ServerRelativeUrl"]
                ),
                "server_relative_url": item[item_type]["ServerRelativeUrl"],
                "type": item_type,
            }
        )
        self.map_document_with_schema(
            document=document, item=item[item_type], document_type=DRIVE_ITEM
        )

        return document

    def format_list_item(
        self,
        item,
    ):
        """Prepare key mappings for list items

        Args:
            item (dictionary): Document from SharePoint.

        Returns:
            dictionary: Modified document with the help of adapter schema.
        """
        document = {"type": LIST_ITEM}

        document.update(
            {  # pyright: ignore
                "_id": item["_id"] if "_id" in item.keys() else item["GUID"],
                "file_name": item.get("file_name", ""),
                "size": int(item.get("Length", 0)),
                "url": item["url"],
            }
        )
        server_url = item.get("server_relative_url")
        if server_url:
            document["server_relative_url"] = server_url

        self.map_document_with_schema(
            document=document, item=item, document_type=LIST_ITEM
        )
        return document

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch SharePoint objects in an async manner.

        Yields:
            dictionary: dictionary containing meta-data of the SharePoint objects.
        """

        server_relative_url = []

        for collection in self.sharepoint_client.site_collections:
            server_relative_url.append(f"/sites/{collection}")
            async for site_data in self.sharepoint_client.get_sites(
                site_url=f"/sites/{collection}"
            ):
                server_relative_url.append(site_data["ServerRelativeUrl"])
                yield self.format_sites(item=site_data), None

        for site_url in server_relative_url:
            async for list_data in self.sharepoint_client.get_lists(site_url=site_url):
                for result in list_data:
                    is_site_page = False
                    selected_field = ""
                    # if BaseType value is 1 then it's document library else it's a list
                    if result.get("BaseType") == 1:
                        if result.get("Title") == "Site Pages":
                            is_site_page = True
                            selected_field = SELECTED_FIELDS
                        yield self.format_lists(
                            item=result, document_type=DOCUMENT_LIBRARY
                        ), None
                        server_url = None
                        func = self.sharepoint_client.get_drive_items
                        format_document = self.format_drive_item
                    else:
                        yield self.format_lists(item=result, document_type=LISTS), None
                        server_url = result["RootFolder"]["ServerRelativeUrl"]
                        func = self.sharepoint_client.get_list_items
                        format_document = self.format_list_item

                    async for item, file_relative_url in func(
                        list_id=result.get("Id"),
                        site_url=result.get("ParentWebUrl"),
                        server_relative_url=server_url,
                        selected_field=selected_field,
                    ):
                        document = format_document(item=item)

                        if file_relative_url is None:
                            yield document, None
                        else:
                            if is_site_page:
                                yield document, partial(
                                    self.sharepoint_client.get_site_pages_content,
                                    document,
                                    item,
                                )
                            else:
                                yield document, partial(
                                    self.get_content,
                                    document,
                                    file_relative_url,
                                    site_url,
                                )

    async def get_content(
            self, document, file_relative_url, site_url, timestamp=None, doit=False
    ):
        """Get content of list items and drive items

        Args:
            document (dictionary): Modified document.
            file_relative_url (str): Relative url of file
            site_url (str): Site path of SharePoint
            timestamp (timestamp, optional): Timestamp of item last modified. Defaults to None.
            doit (boolean, optional): Boolean value for whether to get content or not. Defaults to False.

        Returns:
            dictionary: Content document with id, timestamp & text.
        """
        file_size = int(document["size"])
        if not (doit and file_size):
            return

        filename = (
            document["title"] if document["type"] == "File" else document["file_name"]
        )
        file_extension = self.get_file_extension(filename)
        if not self.can_file_be_downloaded(file_extension, filename, file_size):
            return

        document = {
            "_id": document.get("id"),
            "_timestamp": document.get("_timestamp"),
        }
        return await self.download_and_extract_file(
            document,
            filename,
            file_extension,
            partial(
                self.generic_chunked_download_func,
                partial(
                    self.sharepoint_client._api_call,
                    url_name=ATTACHMENT,
                    host_url=self.sharepoint_client.host_url,
                    value=site_url,
                    file_relative_url=file_relative_url,
                )
            )
        )
