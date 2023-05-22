import aiohttp
import msal

from connectors.source import BaseDataSource
from connectors.utils import url_encode


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

    async def site_collections(self):
        filter_ = url_encode("siteCollection/root ne null")
        select = "siteCollection,webUrl"

        headers = {"authorization": f"Bearer {self._graph_token()}"}

        async with self._http_session.get(
            f"https://graph.microsoft.com/v1.0/sites/?$filter={filter_}&$select={select}",
            headers=headers,
        ) as resp:
            graph_data = await resp.json()
            for item in graph_data["value"]:
                yield item

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
            # load_site_pages(sharepoint_token, site)
            # load_site_drives(access_token, site)
            # load_site_lists(access_token, site)
            # break

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

    # async def _sharepoint_rest_token(self):
    #     url = f"https://accounts.accesscontrol.windows.net/{self._tenant_id}/tokens/OAuth/2"
    #     # GUID in resource is always a constant used to create access token
    #     data = {
    #         "grant_type": "client_credentials",
    #         "resource": f"00000003-0000-0ff1-ce00-000000000000/{self._tenant_name}.sharepoint.com@{self._tenant_id}",
    #         "client_id": f"{self._client_id}@{self._tenant_id}",
    #         "client_secret": self._client_secret,
    #     }
    #     headers = {"Content-Type": "application/x-www-form-urlencoded"}

    #     response = requests.post(url, data=data, headers=headers).json()

    #     return response["access_token"]

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
            print(site_collection)
            site_collection["_id"] = site_collection["webUrl"]
            yield site_collection, None

            async for site in self._client.sites(
                site_collection["siteCollection"]["hostname"]
            ):
                print(site)
                site["_id"] = site["id"]
                yield site, None

    async def ping(self):
        pass

    async def close(self):
        await self._client.close()
