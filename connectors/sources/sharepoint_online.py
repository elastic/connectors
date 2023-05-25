from contextlib import asynccontextmanager

import asyncio
from datetime import datetime, timedelta
from functools import partial, wraps

import aiofiles
import aiohttp
from aiohttp.client_exceptions import ClientResponseError, ServerDisconnectedError
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

class GraphAPIToken:
    def __init__(self, tenant_id, tenant_name, client_id, client_secret):
        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._client_id = client_id
        self._client_secret = client_secret

        self._access_token = None
        self._token_expiration_date = None

    def get(self):
        if self._token_expiration_date and self._token_expiration_date > datetime.now() - timedelta(30):  # TODO: make 30 a constant
            if self._access_token:
                return self._access_token

        # MSAL is not async, sigh
        authority = f"https://login.microsoftonline.com/{self._tenant_id}"
        scope = "https://graph.microsoft.com/.default"

        app = msal.ConfidentialClientApplication(
            client_id=self._client_id,
            client_credential=self._client_secret,
            authority=authority,
        )
        now = datetime.now() 
        result = app.acquire_token_for_client(scopes=[scope])

        if "access_token" in result:
            print(result)
            access_token = result["access_token"]
            expires_in = result["expires_in"]
        
            self._access_token = access_token
            self._token_expiration_date = now + timedelta(expires_in)
            return access_token
        else:
            raise Exception(result.get("error"))

class GraphAPISession:
    BASE_URL = "https://graph.microsoft.com/v1.0/"

    def __init__(self, http_session, graph_api_token):
        self._http_session = http_session
        self._graph_api_token = graph_api_token

    async def fetch(self, relative_url):
        absolute_url = f"{self.BASE_URL}/{relative_url}"

        return await self._get_json(absolute_url)

    async def pipe(self, relative_url, stream):
        absolute_url = f"{self.BASE_URL}/{relative_url}"

        async with self._call_api(
            absolute_url
        ) as resp:
            async for data in resp.content.iter_chunked(1024 * 1024):
                await stream.write(data)

    async def scroll(self, relative_url):
        scroll_url = f"{self.BASE_URL}/{relative_url}"

        while True:
            graph_data = await self._get_json(scroll_url)
            # We're yielding the whole page here, not one item
            yield graph_data["value"] 

            if "@odata.nextLink" in graph_data:
                scroll_url = graph_data["@odata.nextLink"]
            else:
                break

    async def _get_json(self, absolute_url):
        async with self._call_api(
            absolute_url
        ) as resp:
            return await resp.json()

    @asynccontextmanager
    async def _call_api(self, absolute_url):
        while True:
            try:
                headers = {"authorization": f"Bearer {self._graph_api_token.get()}"}

                async with self._http_session.get(
                    absolute_url,
                    headers=headers,
                ) as resp:
                    yield resp
                    return
            except ClientResponseError as e:
                print(f"Got {e.status}")
                if e.status == 429 or e.status == 503:
                    print(e.headers)
                    response_headers = e.headers or {}
                    retry_seconds = None
                    if "Retry-After" in response_headers:
                        retry_seconds = int(response_headers["Retry-After"])
                    else:
                        print(
                            "Response Code from Sharepoint Server is 429 but Retry-After header is not found"
                        )
                        retry_seconds = 30
                    print(
                        f"Rate Limited by Sharepoint: retry in {retry_seconds} seconds"
                    )

                    await asyncio.sleep(retry_seconds)
                else:
                    print(e)
                    raise
            except Exception as e:
                print("Got something else")
                print(e)
                raise


class SharepointOnlineClient:
    def __init__(self, tenant_id, tenant_name, client_id, client_secret):
        self._tenant_id = tenant_id
        self._tenant_name = tenant_name
        self._graph_api_token = GraphAPIToken(tenant_id, tenant_name, client_id, client_secret)

        # Clients
        self._http_session = aiohttp.ClientSession(
            headers={
                "accept": "application/json",
                "content-type": "application/json",
            },
            timeout=aiohttp.ClientTimeout(total=None),
            raise_for_status=True,
        )

        self._graph_api_client = GraphAPISession(self._http_session, self._graph_api_token)

    @profile_time
    async def site_collections(self):
        filter_ = url_encode("siteCollection/root ne null")
        select = "siteCollection,webUrl"

        async for page in self._graph_api_client.scroll(f"sites/?$filter={filter_}&$select={select}"):
            for site_collection in page:
                yield site_collection

    @profile_time
    async def sites(self, site_collection):
        filter_ = ""
        select = ""

        async for page in self._graph_api_client.scroll(f"sites/{site_collection}/sites?$filter={filter_}&search=*&$select={select}"):
            for site in page:
                yield site

    @profile_time
    async def site_drives(self, site_id):
        select = ""

        async for page in self._graph_api_client.scroll(f"sites/{site_id}/drives?$select={select}"):
            for site_drive in page:
                yield site_drive

    @profile_time
    async def drive_items(self, drive_id):
        select = ""

        directory_stack = []

        root = await self._graph_api_client.fetch(f"drives/{drive_id}/root?$select={select}")

        directory_stack.append(root["id"])
        yield root

        while len(directory_stack):
            folder_id = directory_stack.pop()

            async for page in self._graph_api_client.scroll(f"drives/{drive_id}/items/{folder_id}/children?$select={select}"):
                for drive_item in page:
                    if "folder" in drive_item:
                        directory_stack.append(drive_item["id"])
                    yield drive_item

    @profile_time
    async def download_drive_item(self, drive_id, item_id, async_buffer):
        await self._graph_api_client.pipe(f"drives/{drive_id}/items/{item_id}/content", async_buffer)

    @profile_time
    async def site_lists(self, site_id):
        select = ""

        async for page in self._graph_api_client.scroll(f"sites/{site_id}/lists?$select={select}"):
            for site_list in page:
                yield site_list

    @profile_time
    async def site_list_items(self, site_id, list_id):
        select = ""
        expand = "fields"

        async for page in self._graph_api_client.scroll(f"sites/{site_id}/lists/{list_id}/items?$select={select}&$expand={expand}"):
            for site_list in page:
                yield site_list

    @profile_time
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
