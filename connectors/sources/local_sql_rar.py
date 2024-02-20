
from enum import Enum
from functools import cached_property, partial

import aiohttp
from connectors.source import BaseDataSource

class ObjectType(Enum):
  RAR = "RAR"
  WAR = "WAR"

class RESTClient:
    def __init__(self, configuration: dict):
      self.configuration = configuration
      self.base_url = configuration.get('url', 'https://icanhazdadjoke.com/')

    async def make_request(self, url, method='GET', data=None):
        headers = self.configuration.get('headers', {})
        headers['Content-Type'] = 'application/json'
        
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers, json=data) as response:
                response_text = await response.text()
                print(response_text)

    async def ping(self):
      await self.make_request(self.base_url)

class RARCoreDataSource(BaseDataSource):
    """RAR Core"""

    name = "Local RAR Core"
    service_type = "rar_core"

    def __init__(self, configuration):
        """Set up the connection to local DB.

        Args:
            configuration (DataSourceConfiguration): Object of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.client = RESTClient(configuration)

    @classmethod
    def get_default_configuration(cls):
      """Get the default configuration. https://github.com/elastic/connectors/blob/main/docs/DEVELOPING.md#rich-configurable-fields Used by Kibana to generate the UI.

      Returns:
          dictionary: Default configuration.
      """
      return {
          "url": {
              "label": "url",
              "order": 1,
              "type": "str",
              "required": False
          },
      }

    async def ping(self):
      try:
          # await self.client.ping()
          self._logger.debug("Successfully connected to API.")
      except Exception:
          self._logger.exception("Error while connecting to API.")
          raise

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch objects in an async manner.

        The mapping should have least an `id` field
        and optionally a `timestamp` field in ISO 8601 UTC

        Args:
            filtering (optional): Advenced filtering rules. Defaults to None.

        Yields:
            dict, partial: dict containing meta-data of the API objects,
                                partial download content function
        """
        yield self._prepare_doc(), partial(self.get_file, self._prepare_doc())

    async def get_file(self, file, doit=None, timestamp=None):
      """
      Required implementation. Coroutine called within get_docs

      It has two arguments: doit and timestamp
      If doit is False, it should return None immediately.
      If timestamp is provided, it should be used in the mapping.
      """
      
      if not doit:
          return
      return {'TEXT': 'DATA', 'timestamp': timestamp,
              'id': '123'}
    
    def _prepare_doc(data: dict):
      """
      Prepare the document to be indexed.
      """
      return {
         "_id": "444",
          "type": ObjectType.RAR.value,
      }

