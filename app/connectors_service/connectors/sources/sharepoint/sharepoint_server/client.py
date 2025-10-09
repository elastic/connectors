#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import re
from urllib.parse import quote

import httpx
from connectors_sdk.content_extraction import (
    TIKA_SUPPORTED_FILETYPES,
)
from connectors_sdk.logger import logger
from httpx_ntlm import HttpNtlmAuth

from connectors.sources.sharepoint.sharepoint_server.constants import (
    ADMIN_USERS,
    ATTACHMENT,
    ATTACHMENT_DATA,
    BASIC_AUTH,
    DRIVE_ITEM,
    LIST_ITEM,
    LISTS,
    PING,
    RETRY_INTERVAL,
    ROLES,
    ROLES_BY_TITLE_FOR_ITEM,
    ROLES_BY_TITLE_FOR_LIST,
    SITE,
    SITES,
    TOP,
    UNIQUE_ROLES,
    UNIQUE_ROLES_FOR_ITEM,
    URLS,
    USERS,
)
from connectors.utils import (
    CancellableSleeps,
    ssl_context,
)


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
        self.site_collections = []
        for collection in self.configuration["site_collections"]:
            collection_url = (
                collection
                if re.match(r"^https?://", collection)
                else f"{self.host_url}/sites/{collection}"
            )
            self.site_collections.append(collection_url)

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
        if self.session and self.session.is_closed is False:
            return self.session
        self._logger.info("Generating httpx Client Session...")
        request_headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        timeout = httpx.Timeout(timeout=None)  # pyright: ignore

        auth_type = (
            httpx.BasicAuth(
                username=self.configuration["username"],
                password=self.configuration["password"],
            )
            if self.configuration["authentication"] == BASIC_AUTH
            else HttpNtlmAuth(
                username=self.configuration["username"],
                password=self.configuration["password"],
            )
        )
        self.session = httpx.AsyncClient(
            auth=auth_type,
            verify=self.ssl_ctx,
            headers=request_headers,
            timeout=timeout,
        )
        return self.session

    def format_url(
        self,
        site_url,
        relative_url,
        list_item_id=None,
        content_type_id=None,
        is_list_item_has_attachment=False,
    ):
        if is_list_item_has_attachment:
            return (
                site_url
                + quote(relative_url)
                + "/DispForm.aspx?ID="
                + list_item_id
                + "&Source="
                + site_url
                + quote(relative_url)
                + "/AllItems.aspx&ContentTypeId="
                + content_type_id
            )
        else:
            return site_url + quote(relative_url)

    async def close_session(self):
        """Closes unclosed client session"""
        self._sleeps.cancel()
        if self.session is None:
            return
        await self.session.aclose()  # pyright: ignore
        self.session = None

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
                self._get_session()
                result = await self.session.get(  # pyright: ignore
                    url=url,
                    headers=headers,
                )
                if result.is_success is False:
                    msg = f"Error accessing {url}: {result.reason_phrase}"
                    raise Exception(msg)
                if url_name == ATTACHMENT:
                    yield result
                else:
                    yield result.json()
                break
            except Exception as exception:
                if isinstance(
                    exception,
                    httpx.ConnectError,
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

                    next_url = response.get("odata.nextLink", "")  # pyright: ignore
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

                    next_url = response.get("odata.nextLink", "")  # pyright: ignore
            if next_url == "":
                break

    async def _fetch_data_with_query(self, site_url, param_name, **kwargs):
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
                **kwargs,
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
                    site_url=data["Url"]
                ):
                    yield sub_site
                yield data

    async def get_site(self, site_url):
        """Get sites from SharePoint Server

        Args:
            site_url(string): Parent site relative path.
        Yields:
            site_server_url(string): Site path.
        """
        async for response in self._api_call(
            url_name=SITE,
            parent_site_url=site_url,
            host_url=self.host_url,
        ):
            yield response

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
            for site_list in list_data:
                yield site_list

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

    async def get_list_items(
        self, list_id, site_url, server_relative_url, list_relative_url, **kwargs
    ):
        """This method fetches items from all the lists in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            server_relative_url(string): Relative url of site.
            list_relative_url(string): Relative url of list.
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
                        site_url=site_url,
                        relative_url=list_relative_url,
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
                        site_url=site_url,
                        relative_url=self.fix_relative_url(
                            server_relative_url,
                            attachment_file.get("ServerRelativeUrl"),
                        ),
                    )
                    result["file_name"] = attachment_file.get("FileName")
                    result["server_relative_url"] = attachment_file["ServerRelativeUrl"]

                    file_relative_url = self.verify_filename_for_extraction(
                        filename=attachment_file["FileName"],
                        relative_url=file_relative_url,
                    )

                    yield result, file_relative_url

    async def get_drive_items(self, list_id, site_url, **kwargs):
        """This method fetches items from all the drives in a collection.

        Args:
            list_id(string): List id.
            site_url(string): Site path.
            site_relative_url(string): Relative url of site
            list_relative_url(string): Relative url of list
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
        site_url = ""
        if len(self.site_collections) > 0:
            site_url = self.site_collections[0]
        else:
            site_url = self.host_url
        await anext(
            self._api_call(
                url_name=PING,
                site_url=site_url,
                host_url=self.host_url,
            )
        )

    async def fetch_users(self):
        for site_url in self.site_collections:
            async for users in self._fetch_data_with_query(
                site_url=site_url, param_name=USERS
            ):
                for user in users:
                    yield user

    async def site_role_assignments(self, site_url):
        async for roles in self._fetch_data_with_query(
            site_url=site_url, param_name=ROLES
        ):
            for role in roles:
                yield role

    async def site_admins(self, site_url):
        async for users in self._fetch_data_with_query(
            site_url=site_url, param_name=ADMIN_USERS
        ):
            for user in users:
                yield user

    async def site_role_assignments_using_title(self, site_url, site_list_name):
        async for roles in self._fetch_data_with_query(
            site_url=site_url,
            param_name=ROLES_BY_TITLE_FOR_LIST,
            site_list_name=site_list_name,
        ):
            for role in roles:
                yield role

    async def site_list_has_unique_role_assignments(self, site_list_name, site_url):
        role = await anext(
            self._api_call(
                url_name=UNIQUE_ROLES,
                parent_site_url=site_url,
                host_url=self.host_url,
                site_list_name=site_list_name,
            )
        )
        return role.get("value", False)

    async def site_list_item_has_unique_role_assignments(
        self, site_url, site_list_name, list_item_id
    ):
        role = await anext(
            self._api_call(
                url_name=UNIQUE_ROLES_FOR_ITEM,
                parent_site_url=site_url,
                host_url=self.host_url,
                site_list_name=site_list_name,
                list_item_id=list_item_id,
            )
        )
        return role.get("value", False)

    async def site_list_item_role_assignments(
        self, site_url, site_list_name, list_item_id
    ):
        async for roles in self._fetch_data_with_query(
            site_url=site_url,
            param_name=ROLES_BY_TITLE_FOR_ITEM,
            site_list_name=site_list_name,
            list_item_id=list_item_id,
        ):
            for role in roles:
                yield role

    def fix_relative_url(self, site_relative_url, item_relative_url):
        if item_relative_url is not None:
            item_relative_url = (
                item_relative_url
                if site_relative_url == "/"
                else item_relative_url.replace(site_relative_url, "")
            )
        return item_relative_url
