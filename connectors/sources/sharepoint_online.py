import asyncio
from datetime import datetime
from functools import partial

import aiofiles
import aiohttp
import msal
from aiofiles.os import remove
from aiofiles.tempfile import NamedTemporaryFile

from connectors.source import BaseDataSource
from connectors.utils import (
    TIKA_SUPPORTED_FILETYPES,
    convert_to_b64,
    get_pem_format,
    url_encode,
)


def profile_time(f):
    def wrapper(*args, **kargs):
        before = datetime.now()
        result = f(*args, **kargs)
        after = datetime.now()
        print(
            f"Took {(after - before).total_seconds() * 1000} milliseconds to call {f.__name__}"
        )
        return result

    return wrapper


class SharepointOnlineClient:
    def __init__(self, tenant_id, tenant_name, client_id, client_secret):
        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._client_id = client_id
        self._client_secret = client_secret

        # Clients
        self._http_session = aiohttp.ClientSession(
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

    @profile_time
    async def site_collections(self):
        filter_ = url_encode("siteCollection/root ne null")
        select = "siteCollection,webUrl"

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        fetch_link = f"https://graph.microsoft.com/v1.0/sites/?$filter={filter_}&$select={select}"

        while fetch_link:
            async with self._http_session.get(
                fetch_link,
                headers=headers,
            ) as resp:
                graph_data = await resp.json()
                if "@odata.nextLink" in graph_data:
                    fetch_link = graph_data["@odata.nextLink"]
                else:
                    fetch_link = None
                for item in graph_data["value"]:
                    yield item

    @profile_time
    async def sites(self, site_collection):
        filter_ = ""
        select = ""

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_collection}/sites?$filter={filter_}&search=*&$select={select}",
            headers=headers,
        ) as resp:
            graph_data = await resp.json()

        for site in graph_data["value"]:
            yield site

    @profile_time
    async def site_drives(self, site_id):
        select = ""

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select={select}",
            headers=headers,
        ) as resp:
            graph_data = await resp.json()

            for site_drive in graph_data["value"]:
                yield site_drive

    @profile_time
    async def drive_items(self, drive_id):
        select = ""

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        directory_stack = []

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root?$select={select}",
            headers=headers,
        ) as resp:
            root_graph_data = await resp.json()
            directory_stack.append(root_graph_data["id"])
            yield root_graph_data

        while len(directory_stack):
            folder_id = directory_stack.pop()

            fetch_link = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}/children?$select={select}"

            while fetch_link:
                async with self._http_session.get(
                    fetch_link,
                    headers=headers,
                ) as resp:
                    graph_data = await resp.json()
                    if "@odata.nextLink" in graph_data:
                        fetch_link = graph_data["@odata.nextLink"]
                    else:
                        fetch_link = None
                    children_graph_data = await resp.json()
                    for child_item in children_graph_data["value"]:
                        if "folder" in child_item:
                            directory_stack.append(child_item["id"])
                        yield child_item

    @profile_time
    async def download_drive_item(self, drive_id, item_id, async_buffer):
        headers = {
            "authorization": f"Bearer {self._graph_token()}",
            "content-type": "text/plain",
        }

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}/content",
            headers=headers,
        ) as resp:
            async for data in resp.content.iter_chunked(1024 * 1024):
                await async_buffer.write(data)

    @profile_time
    async def site_lists(self, site_id):
        select = ""

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$select={select}",
            headers=headers,
        ) as resp:
            graph_data = await resp.json()

            for site_list in graph_data["value"]:
                yield site_list

    async def site_list_items(self, site_id, list_id):
        select = ""
        expand = "fields"

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$select={select}&$expand={expand}"

        async with self._http_session.get(
            url,
            headers=headers,
        ) as resp:
            graph_data = await resp.json()

            for site_list_item in graph_data["value"]:
                yield site_list_item

    async def site_page(self, site_url, filename):
        select = "Title,CanvasContent1,FileLeafRef"

        rest_token = await self._sharepoint_rest_token()
        headers = {"authorization": f"Bearer {rest_token}"}

        async with self._http_session.get(
            f"{site_url}/_api/web/lists/getbytitle('Site%20Pages')/items?$select={select}&$filter=FileLeafRef eq '{filename}'",
            headers=headers,
        ) as resp:
            graph_data = await resp.json()

            for site_page in graph_data["value"]:
                yield site_page

    def _graph_token(self):
        # MSAL is not async, sigh
        authority = f"https://login.microsoftonline.com/{self._tenant_id}"
        scope = "https://graph.microsoft.com/.default"

        app = msal.ConfidentialClientApplication(
            client_id=self._client_id,
            client_credential=self._client_secret,
            authority=authority,
        )

        result = app.acquire_token_for_client(scopes=[scope])

        if "access_token" in result:
            access_token = result["access_token"]
            return access_token
        else:
            raise Exception(result.get("error"))

    async def _sharepoint_rest_token(self):
        # GUID in resource is always a constant used to create access token
        data = {
            "grant_type": "client_credentials",
            "resource": f"00000003-0000-0ff1-ce00-000000000000/{self._tenant_name}.sharepoint.com@{self._tenant_id}",
            "client_id": f"{self._client_id}@{self._tenant_id}",
            "client_secret": self._client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        async with self._http_session.get(
            f"https://accounts.accesscontrol.windows.net/{self._tenant_id}/tokens/OAuth/2",
            headers=headers,
            data=data,
        ) as resp:
            data = await resp.json()
            return data["access_token"]

    async def close(self):
        await self._http_session.close()


class SharepointOnlineDataSource(BaseDataSource):
    """Sharepoint Online"""

    name = "Sharepoint Online"
    service_type = "sharepoint_online"

    def __init__(self, configuration):
        super().__init__(configuration=configuration)

        tenant_id = self.configuration["tenant_id"]
        tenant_name = self.configuration["tenant_name"]
        client_id = self.configuration["client_id"]
        client_secret = self.configuration["secret_value"]

        self._client = SharepointOnlineClient(
            tenant_id, tenant_name, client_id, client_secret
        )

    @classmethod
    def get_default_configuration(cls):
        return {
            "tenant_id": {
                "label": "Tenant Id",
                "order": 1,
                "type": "str",
                "value": "",
            },
            "tenant_name": {  # TODO: actually call graph api for this
                "label": "Tenant Name",
                "order": 2,
                "type": "str",
                "value": "",
            },
            "client_id": {
                "label": "Client Id",
                "order": 3,
                "sensitive": True,
                "type": "str",
                "value": "",
                "required": False,
            },
            "secret_value": {
                "label": "Secret Value",
                "order": 4,
                "sensitive": True,
                "type": "str",
                "value": "",
                "required": False,
            },
        }

    async def validate_config(self):
        pass

    async def get_docs(self, filtering=None):
        async for site_collection in self._client.site_collections():
            site_collection["_id"] = site_collection["webUrl"]
            site_collection["object_type"] = "site_collection"
            yield site_collection, None

            async for site in self._client.sites(
                site_collection["siteCollection"]["hostname"]
            ):
                site["_id"] = site["id"]
                site["object_type"] = "site"
                yield site, None

                async for site_drive in self._client.site_drives(site["id"]):
                    site_drive["_id"] = site_drive["id"]
                    site_drive["object_type"] = "site_drive"
                    yield site_drive, None

                    async for drive_item in self._client.drive_items(site_drive["id"]):
                        drive_item["_id"] = drive_item["id"]
                        drive_item["_timestamp"] = drive_item["lastModifiedDateTime"]
                        drive_item["object_type"] = "drive_item"

                        download_func = None

                        if "@microsoft.graph.downloadUrl" in drive_item:
                            if "size" in drive_item and drive_item["size"] < 10485760:
                                download_func = partial(self.get_content, drive_item)
                            else:
                                print(
                                    f"Not downloading file {drive_item['name']} of size {drive_item['size']}"
                                )

                        yield drive_item, download_func

                async for site_list in self._client.site_lists(site["id"]):
                    site_list["_id"] = site_list["id"]
                    site_list["object_type"] = "site_list"

                    yield site_list, None

                    async for list_item in self._client.site_list_items(
                        site["id"], site_list["id"]
                    ):
                        list_item["_id"] = list_item["id"]
                        list_item["object_type"] = "list_item"

                        download_func = None

                        yield list_item, download_func

    @profile_time
    async def get_content(self, drive_item, timestamp=None, doit=False):
        document_size = int(drive_item["size"])

        # if not (doit and document_size):
        #     return

        if document_size > 10485760:
            return

        result = {
            "_id": drive_item["id"],
            "_timestamp": drive_item["lastModifiedDateTime"],
        }

        source_file_name = ""
        async with NamedTemporaryFile(mode="wb", delete=False) as async_buffer:
            await self._client.download_drive_item(
                drive_item["parentReference"]["driveId"], drive_item["id"], async_buffer
            )

            source_file_name = async_buffer.name

        await asyncio.to_thread(
            convert_to_b64,
            source=source_file_name,
        )
        async with aiofiles.open(file=source_file_name, mode="r") as target_file:
            # base64 on macOS will add a EOL, so we strip() here
            content = (await target_file.read()).strip()
            result["_attachment"] = content

        return result

    async def ping(self):
        pass

    async def close(self):
        await self._client.close()
