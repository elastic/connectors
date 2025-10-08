#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#

import os
import re

import aiohttp
from connectors_sdk.logger import logger
from connectors.utils import url_encode
from connectors.sources.sharepoint.sharepoint_online.constants import (
    DEFAULT_PARALLEL_CONNECTION_COUNT,
    GRAPH_API_AUTH_URL,
    GRAPH_API_URL,
    WILDCARD,
    DRIVE_ITEMS_FIELDS,
    DELTA_LINK_KEY
)
from connectors.sources.sharepoint.sharepoint_online.utils import (
    MicrosoftAPISession,
    NotFound,
    BadRequestError,
    InternalServerError,
    InvalidSharepointTenant,
    PermissionsMissing,
    GraphAPIToken,
    SharepointRestAPIToken,
    EntraAPIToken,
    DriveItemsPage,
)


class SharepointOnlineClient:
    def __init__(
        self,
        tenant_id,
        tenant_name,
        client_id,
        client_secret=None,
        certificate=None,
        private_key=None,
    ):
        # Sharepoint / Graph API has quite strict throttling policies
        # If connector is overzealous, it can be banned for not respecting throttling policies
        # However if connector has a low setting for the tcp_connector limit, then it'll just be slow.
        # Change the value at your own risk
        tcp_connector = aiohttp.TCPConnector(limit=DEFAULT_PARALLEL_CONNECTION_COUNT)
        self._http_session = aiohttp.ClientSession(  # TODO: lazy create this
            connector=tcp_connector,
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._tenant_name_pattern = re.compile(
            "https://(.*).sharepoint.com"
        )  # Used later for url validation

        if client_secret and not certificate and not private_key:
            self.graph_api_token = GraphAPIToken(
                self._http_session, tenant_id, tenant_name, client_id, client_secret
            )
            self.rest_api_token = SharepointRestAPIToken(
                self._http_session, tenant_id, tenant_name, client_id, client_secret
            )
        elif certificate and private_key:
            self.graph_api_token = EntraAPIToken(
                self._http_session,
                tenant_id,
                tenant_name,
                client_id,
                certificate,
                private_key,
                "https://graph.microsoft.com/.default",
            )
            self.rest_api_token = EntraAPIToken(
                self._http_session,
                tenant_id,
                tenant_name,
                client_id,
                certificate,
                private_key,
                f"https://{self._tenant_name}.sharepoint.com/.default",
            )
        else:
            msg = "Unexpected authentication: either a client_secret or certificate+private_key should be provided"
            raise Exception(msg)

        self._logger = logger

        self._graph_api_client = MicrosoftAPISession(
            self._http_session, self.graph_api_token, "@odata.nextLink", self._logger
        )
        self._rest_api_client = MicrosoftAPISession(
            self._http_session, self.rest_api_token, "odata.nextLink", self._logger
        )

    def set_logger(self, logger_):
        self._logger = logger_
        self._graph_api_client.set_logger(self._logger)
        self._rest_api_client.set_logger(self._logger)

    async def groups(self):
        select = ""

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/groups?$select={select}"
        ):
            for group in page:
                yield group

    async def group_sites(self, group_id):
        select = ""

        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/groups/{group_id}/sites?$select={select}"
            ):
                for group_site in page:
                    yield group_site
        except NotFound:
            # We can safely ignore cause Sharepoint can return 404 in case List Item is of specific types that do not support/have attachments
            # Yes, makes no sense to me either.
            return

    async def site_collections(self):
        try:
            filter_ = url_encode("siteCollection/root ne null")
            select = "siteCollection,webUrl"

            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/?$filter={filter_}&$select={select}"
            ):
                for site_collection in page:
                    yield site_collection
        except PermissionsMissing:
            self._logger.warning(
                "Looks like 'Sites.Read.All' permission is missing to fetch all root-level site collections, hence fetching only tenant root site"
            )
            yield await self._graph_api_client.fetch(url=f"{GRAPH_API_URL}/sites/root")

    async def site_role_assignments(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)
        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/web/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            self._logger.debug(f"No role assignments found for site: '{site_web_url}'")
            return

    async def site_admins(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)
        filter_param = url_encode("isSiteAdmin eq true")
        url = f"{site_web_url}/_api/web/SiteUsers?$filter={filter_param}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for member in page:
                    yield member
        except NotFound:
            self._logger.debug(f"No site admins found for site: '${site_web_url}'")
            return

    async def site_groups_users(self, site_web_url, site_group_id):
        self._validate_sharepoint_rest_url(site_web_url)

        select_ = "Email,Id,UserPrincipalName,LoginName,Title"
        url = f"{site_web_url}/_api/web/sitegroups/getbyid({site_group_id})/users?$select={select_}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for site_group_user in page:
                    yield site_group_user
        except NotFound:
            self._logger.warning(
                f"NotFound error when fetching users for sitegroup '{site_group_id}' at '{site_web_url}'."
            )
            return

    async def active_users_with_groups(self):
        expand = "transitiveMemberOf($select=id)"
        top = 999  # this is accepted, but does not get taken literally. Response size seems to max out at 100
        filter_ = "accountEnabled eq true"
        select = "UserName,userPrincipalName,Email,mail,transitiveMemberOf,id,createdDateTime"
        url = f"{GRAPH_API_URL}/users?$expand={expand}&$top={top}&$filter={filter_}&$select={select}"

        try:
            async for page in self._graph_api_client.scroll(url):
                for user in page:
                    yield user
        except NotFound:
            return

    async def group_members(self, group_id):
        url = f"{GRAPH_API_URL}/groups/{group_id}/members"

        try:
            async for page in self._graph_api_client.scroll(url):
                for member in page:
                    yield member
        except NotFound:
            return

    async def group_owners(self, group_id):
        select = "id,mail,userPrincipalName"
        url = f"{GRAPH_API_URL}/groups/{group_id}/owners?$select={select}"

        try:
            async for page in self._graph_api_client.scroll(url):
                for owner in page:
                    yield owner
        except NotFound:
            return

    async def site_users(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/web/siteusers"

        try:
            async for page in self._rest_api_client.scroll(url):
                for user in page:
                    yield user
        except NotFound:
            return

    async def sites(
        self,
        sharepoint_host,
        allowed_root_sites,
        enumerate_all_sites=True,
        fetch_subsites=False,
    ):
        if allowed_root_sites == [WILDCARD] or enumerate_all_sites:
            self._logger.debug(f"Looking up all sites to fetch: {allowed_root_sites}")
            async for site in self._all_sites(sharepoint_host, allowed_root_sites):
                yield site
        else:
            self._logger.debug(f"Looking up individual sites: {allowed_root_sites}")
            for allowed_site in allowed_root_sites:
                try:
                    if fetch_subsites:
                        async for site in self._fetch_site_and_subsites_by_path(
                            sharepoint_host, allowed_site
                        ):
                            yield site
                    else:
                        yield await self._fetch_site(sharepoint_host, allowed_site)

                except NotFound:
                    self._logger.warning(
                        f"Could not look up site '{allowed_site}' by relative path in parent site: {sharepoint_host}"
                    )

    async def _all_sites(self, sharepoint_host, allowed_root_sites):
        select = ""
        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/{sharepoint_host}/sites?search=*&$select={select}"
            ):
                for site in page:
                    # Filter out site collections that are not needed
                    if [WILDCARD] != allowed_root_sites and site[
                        "name"
                    ] not in allowed_root_sites:
                        continue
                    yield site
        except PermissionsMissing as exception:
            if allowed_root_sites == [WILDCARD]:
                msg = "The configuration field 'Comma-separated list of sites' with '*' value is only compatible with 'Sites.Read.All' permission."
            else:
                msg = "To enumerate all sites, the connector requires 'Sites.Read.All' permission"
            raise PermissionsMissing(msg) from exception

    async def _fetch_site_and_subsites_by_path(self, sharepoint_host, allowed_site):
        self._logger.debug(
            f"Requesting site '{allowed_site}' and subsites by relative path in host: {sharepoint_host}"
        )
        site_with_subsites = await self._graph_api_client.fetch(
            f"{GRAPH_API_URL}/sites/{sharepoint_host}:/sites/{allowed_site}?expand=sites"
        )
        async for site in self._recurse_sites(site_with_subsites):
            yield site

    async def _fetch_site(self, sharepoint_host, allowed_site):
        self._logger.debug(
            f"Requesting site '{allowed_site}' by relative path in parent site: {sharepoint_host}"
        )
        return await self._graph_api_client.fetch(
            f"{GRAPH_API_URL}/sites/{sharepoint_host}:/sites/{allowed_site}"
        )

    async def _scroll_subsites_by_parent_id(self, parent_site_id):
        self._logger.debug(f"Scrolling subsites of {parent_site_id}")
        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{parent_site_id}/sites?expand=sites"
        ):
            for site in page:
                async for subsite in self._recurse_sites(site):  # pyright: ignore
                    yield subsite

    async def _recurse_sites(self, site_with_subsites):
        subsites = site_with_subsites.pop("sites", [])
        site_with_subsites.pop("sites@odata.context", None)  # remove unnecessary field
        yield site_with_subsites
        if subsites:
            async for site in self._scroll_subsites_by_parent_id(
                site_with_subsites["id"]
            ):
                yield site

    async def site_drives(self, site_id):
        select = "createdDateTime,description,id,lastModifiedDateTime,name,webUrl,driveType,createdBy,lastModifiedBy,owner"

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{site_id}/drives?$select={select}"
        ):
            for site_drive in page:
                yield site_drive

    async def drive_items_delta(self, url):
        async for response in self._graph_api_client.scroll_delta_url(url):
            delta_link = (
                response[DELTA_LINK_KEY] if DELTA_LINK_KEY in response else None
            )
            if "value" in response and len(response["value"]) > 0:
                yield DriveItemsPage(response["value"], delta_link)

    async def drive_items(self, drive_id, url=None):
        url = (
            (
                f"{GRAPH_API_URL}/drives/{drive_id}/root/delta?$select={DRIVE_ITEMS_FIELDS}"
            )
            if not url
            else url
        )

        async for page in self.drive_items_delta(url):
            yield page

    async def drive_items_permissions_batch(self, drive_id, drive_item_ids):
        requests = []

        for item_id in drive_item_ids:
            permissions_uri = f"/drives/{drive_id}/items/{item_id}/permissions"
            requests.append({"id": item_id, "method": "GET", "url": permissions_uri})

        if not requests:
            self._logger.debug(
                "Skipping fetching empty batch of drive item permissions"
            )
            return
        try:
            batch_url = f"{GRAPH_API_URL}/$batch"
            batch_request = {"requests": requests}
            batch_response = await self._graph_api_client.post(batch_url, batch_request)

            for response in batch_response.get("responses", []):
                yield response
        except NotFound:
            return

    async def download_drive_item(self, drive_id, item_id, async_buffer):
        await self._graph_api_client.pipe(
            f"{GRAPH_API_URL}/drives/{drive_id}/items/{item_id}/content", async_buffer
        )

    async def site_lists(self, site_id):
        select = "createdDateTime,id,lastModifiedDateTime,name,webUrl,displayName,createdBy,lastModifiedBy"

        try:
            async for page in self._graph_api_client.scroll(
                f"{GRAPH_API_URL}/sites/{site_id}/lists?$select={select}"
            ):
                for site_list in page:
                    yield site_list
        except NotFound:
            return

    async def site_list_has_unique_role_assignments(self, site_web_url, site_list_name):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False

    async def site_list_role_assignments(self, site_web_url, site_list_name):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def site_list_item_has_unique_role_assignments(
        self, site_web_url, site_list_name, list_item_id
    ):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False
        except BadRequestError:
            self._logger.warning(
                f"Received error response when retrieving `{list_item_id}` from list: `{site_list_name}` in site: `{site_web_url}`"
            )
            return False

    async def site_list_item_role_assignments(
        self, site_web_url, site_list_name, list_item_id
    ):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/lists/GetByTitle('{site_list_name}')/items({list_item_id})/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def site_list_items(self, site_id, list_id):
        select = "createdDateTime,id,lastModifiedDateTime,weburl,createdBy,lastModifiedBy,contentType"
        expand = "fields($select=Title,Link,Attachments,LinkTitle,LinkFilename,Description,Conversation)"

        async for page in self._graph_api_client.scroll(
            f"{GRAPH_API_URL}/sites/{site_id}/lists/{list_id}/items?$select={select}&$expand={expand}"
        ):
            for site_list in page:
                yield site_list

    async def site_list_item_attachments(self, site_web_url, list_title, list_item_id):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/lists/GetByTitle('{list_title}')/items({list_item_id})?$expand=AttachmentFiles"

        try:
            list_item = await self._rest_api_client.fetch(url)

            for attachment in list_item["AttachmentFiles"]:
                yield attachment
        except NotFound:
            # We can safely ignore cause Sharepoint can return 404 in case List Item is of specific types that do not support/have attachments
            # Yes, makes no sense to me either.
            return

    async def download_attachment(self, attachment_absolute_path, async_buffer):
        self._validate_sharepoint_rest_url(attachment_absolute_path)

        await self._rest_api_client.pipe(
            f"{attachment_absolute_path}/$value", async_buffer
        )

    async def site_pages(self, site_web_url):
        self._validate_sharepoint_rest_url(site_web_url)

        # select = "Id,Title,LayoutWebpartsContent,CanvasContent1,Description,Created,AuthorId,Modified,EditorId"
        select = "*,EncodedAbsUrl"  # ^ is what we want, but site pages don't have consistent schemas, and this causes errors. Better to fetch all and slice
        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site%20Pages')/items?$select={select}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for site_page in page:
                    yield {
                        "Id": site_page.get("Id"),
                        "Title": site_page.get("Title"),
                        "webUrl": site_page.get("EncodedAbsUrl"),
                        "LayoutWebpartsContent": site_page.get("LayoutWebpartsContent"),
                        "CanvasContent1": site_page.get("CanvasContent1"),
                        "WikiField": site_page.get("WikiField"),
                        "Description": site_page.get("Description"),
                        "Created": site_page.get("Created"),
                        "AuthorId": site_page.get("AuthorId"),
                        "Modified": site_page.get("Modified"),
                        "EditorId": site_page.get("EditorId"),
                        "odata.id": site_page.get("odata.id"),
                        "OData__UIVersionString": site_page.get(
                            "OData__UIVersionString"
                        ),
                    }
        except NotFound:
            # I'm not sure if site can have no pages, but given how weird API is I put this here
            # Just to be on a safe side
            return

    async def site_page_has_unique_role_assignments(self, site_web_url, site_page_id):
        self._validate_sharepoint_rest_url(site_web_url)

        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site Pages')/items('{site_page_id}')/HasUniqueRoleAssignments"

        try:
            response = await self._rest_api_client.fetch(url)
            return response.get("value", False)
        except NotFound:
            return False

    async def site_page_role_assignments(self, site_web_url, site_page_id):
        self._validate_sharepoint_rest_url(site_web_url)

        expand = "Member/users,RoleDefinitionBindings"

        url = f"{site_web_url}/_api/web/lists/GetByTitle('Site Pages')/items('{site_page_id}')/roleassignments?$expand={expand}"

        try:
            async for page in self._rest_api_client.scroll(url):
                for role_assignment in page:
                    yield role_assignment
        except NotFound:
            return

    async def users_and_groups_for_role_assignment(self, site_web_url, role_assignment):
        self._validate_sharepoint_rest_url(site_web_url)

        if "PrincipalId" not in role_assignment:
            return []

        principal_id = role_assignment["PrincipalId"]

        url = f"{site_web_url}/_api/web/GetUserById('{principal_id}')"

        try:
            return await self._rest_api_client.fetch(url)
        except NotFound:
            return []
        except InternalServerError:
            # This can also mean "not found" so handling it explicitly
            return []

    async def groups_user_transitive_member_of(self, user_id):
        url = f"{GRAPH_API_URL}/users/{user_id}/transitiveMemberOf"

        try:
            async for page in self._graph_api_client.scroll(url):
                for group in page:
                    yield group
        except NotFound:
            return

    async def tenant_details(self):
        url = f"{GRAPH_API_AUTH_URL}/common/userrealm/?user=cj@{self._tenant_name}.onmicrosoft.com&api-version=2.1&checkForMicrosoftAccount=false"

        return await self._rest_api_client.fetch(url)

    def _validate_sharepoint_rest_url(self, url):
        # TODO: make it better suitable for ftest
        if "OVERRIDE_URL" in os.environ:
            return

        # I haven't found a better way to validate tenant name for now.
        actual_tenant_name = self._tenant_name_pattern.findall(url)[0]

        if self._tenant_name != actual_tenant_name:
            msg = f"Unable to call Sharepoint REST API - tenant name is invalid. Authenticated for tenant name: {self._tenant_name}, actual tenant name for the service: {actual_tenant_name}. For url: {url}"
            raise InvalidSharepointTenant(msg)

    async def close(self):
        await self._http_session.close()
        self._graph_api_client.close()
        self._rest_api_client.close()
