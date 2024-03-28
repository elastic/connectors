#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GraphQL source module responsible to fetch documents based on GraphQL Query."""
import json
import re
from copy import deepcopy
from functools import cached_property

import aiohttp
from aiohttp.client_exceptions import ClientResponseError
from graphql import parse, visit
from graphql.language.ast import VariableNode
from graphql.language.visitor import Visitor

from connectors.logger import logger
from connectors.source import BaseDataSource, ConfigurableFieldValueError
from connectors.utils import (
    CancellableSleeps,
    RetryStrategy,
    hash_id,
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2

# Regular expression to validate the Base URL
URL_REGEX = "^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)$"

PING_QUERY = """
{
  __schema {
    queryType {
      name
    }
  }
}
"""


class FieldVisitor(Visitor):
    fields_dict = {}
    variables_dict = {}

    def enter_field(self, node, *args):
        self.fields_dict[node.name.value] = []
        self.variables_dict[node.name.value] = {}
        if node.arguments:
            for arg in node.arguments:
                self.fields_dict[node.name.value].append(arg.name.value)
                if isinstance(arg.value, VariableNode):
                    self.variables_dict[node.name.value][
                        arg.name.value
                    ] = arg.value.name.value


class UnauthorizedException(Exception):
    pass


class GraphQLClient:
    def __init__(self, configuration):
        self._sleeps = CancellableSleeps()
        self.configuration = configuration
        self._logger = logger
        self.graphql_query = self.configuration["graphql_query"]
        self.url = self.configuration["http_endpoint"]
        self.graphql_object_list = self.configuration["graphql_object_list"]
        self.http_method = self.configuration["http_method"]
        self.authentication_method = self.configuration["authentication_method"]
        self.pagination_model = self.configuration["pagination_model"]
        self.pagination_key = self.configuration["pagination_key"]
        self.valid_objects = []
        self.variables = {}
        self.headers = {}

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        timeout = aiohttp.ClientTimeout(total=self.configuration["connection_timeout"])
        if self.authentication_method == "bearer":
            self.headers.update(
                {
                    "Authorization": f"Bearer {self.configuration['token']}",
                }
            )
            return aiohttp.ClientSession(
                headers=self.headers,
                timeout=timeout,
                raise_for_status=True,
            )
        elif self.authentication_method == "basic":
            basic_auth = aiohttp.BasicAuth(
                login=self.configuration["username"],
                password=self.configuration["password"],
            )
            return aiohttp.ClientSession(
                auth=basic_auth,
                headers=self.headers,
                timeout=timeout,
                raise_for_status=True,
            )
        else:
            return aiohttp.ClientSession(
                headers=self.headers,
                timeout=timeout,
                raise_for_status=True,
            )

    def extract_graphql_data_items(self, data):
        """Returns sub objects from the response based on graphql_object_list

        Args:
            data (dict): data to extract

        Yields:
            dictionary/list: Documents from the response
        """
        for keys in self.graphql_object_list:
            current_level_data = data
            key_list = keys.split(".")

            for key in key_list:
                if isinstance(current_level_data, dict):
                    if key not in current_level_data.keys():
                        msg = (
                            f"{key} not found in the response. Please check the query."
                        )
                        raise ConfigurableFieldValueError(msg)
                    current_level_data = current_level_data.get(key, {})
                else:
                    msg = "Found list while processing GraphQL Objects List. Please check the query and provide path upto dictionary."
                    raise ConfigurableFieldValueError(msg)

            if isinstance(current_level_data, dict):
                if current_level_data:
                    yield current_level_data

            if isinstance(current_level_data, list):
                for doc in current_level_data:
                    yield doc

    def extract_pagination_info(self, data):
        pagination_key_path = self.pagination_key.split(".")
        for key in pagination_key_path:
            if isinstance(data, dict):
                if key not in data.keys():
                    msg = f"Pagination key: {key} not found in the response. Please check the query."
                    raise ConfigurableFieldValueError(msg)
                data = data.get(key)
            else:
                msg = "Found list while processing Pagination key. Please check the query and provide path upto dictionary."
                raise ConfigurableFieldValueError(msg)

        if (
            isinstance(data, dict)
            and data.get("pageInfo")
            and {"hasNextPage", "endCursor"}.issubset(set(data.get("pageInfo", {})))
        ):
            page_info = data.get("pageInfo")
            has_next_page = page_info.get("hasNextPage")
            end_cursor = page_info.get("endCursor")

            return has_next_page, end_cursor, pagination_key_path[-1]
        else:
            msg = "Pagination is enabled but pageInfo field is missing with hasNextPage and endCursor in the query, please add pageInfo field and add hasNextPage and endCursor inside it."
            raise ConfigurableFieldValueError(msg)

    def validate_query(self, graphql_query, visitor):
        graphql_object = self.pagination_key.split(".")[-1]
        self._logger.debug(f"Finding pageInfo field in {graphql_object}.")
        if not (
            {"after"}.issubset(set(visitor.fields_dict.get(graphql_object, [])))
            and "pageInfo" in visitor.fields_dict.keys()
        ):
            msg = f"Pagination is enabled but pageInfo not found. Please add pageInfo field inside {graphql_object} and after argument in {graphql_object}."
            raise ConfigurableFieldValueError(msg)

    async def paginated_call(self, graphql_query):
        if self.pagination_model == "cursor_pagination":
            ast = parse(graphql_query)  # pyright: ignore
            visitor = FieldVisitor()
            visit(ast, visitor)  # pyright: ignore

            self.validate_query(graphql_query, visitor)
            while True:
                self._logger.debug(
                    f"Fetching document with variables: {self.variables} and query: {graphql_query}."
                )

                has_new_page = False
                data = await self.make_request(graphql_query)
                (
                    has_next_page,
                    end_cursor,
                    pagination_key,
                ) = self.extract_pagination_info(data)

                if has_next_page and end_cursor:
                    self.variables[
                        visitor.variables_dict[pagination_key]["after"]
                    ] = end_cursor
                    has_new_page = True

                for documents in self.extract_graphql_data_items(data=data):
                    yield documents

                if not has_new_page:
                    return

    @retryable(
        retries=RETRIES,
        interval=RETRY_INTERVAL,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
    )
    async def make_request(self, graphql_query):
        try:
            if self.http_method == "get":
                return await self.get(graphql_query=graphql_query)
            else:
                return await self.post(graphql_query=graphql_query)
        except ClientResponseError as exception:
            if exception.status == 401:
                msg = "Provided credentials or token do not have the necessary permissions to perform the request."
                raise UnauthorizedException(msg) from exception
            else:
                raise
        except Exception:
            raise

    async def get(self, graphql_query):
        params = {"query": graphql_query}
        async with self.session.get(url=self.url, params=params) as response:
            json_response = await response.json()
            if not json_response.get("errors"):
                data = json_response.get("data", {})
                return data
            msg = f"Error while executing query. Exception: {json_response['errors']}"
            raise Exception(msg)

    async def post(self, graphql_query):
        """Invoke GraphQL request to fetch response.

        Args:
            graphql_query (string): GraphQL query

        Raises:
            UnauthorizedException: Unauthorized exception.
            ClientResponseError: ClientResponseError exception.
            exception: An instance of an exception class.

        Yields:
            dictionary: Client response
        """
        query_data = {
            "query": graphql_query,
            "variables": self.variables,
        }
        async with self.session.post(url=self.url, json=query_data) as response:
            json_response = await response.json()
            if not json_response.get("errors"):
                data = json_response.get("data", {})
                return data
            msg = f"Error while executing query. Exception: {json_response['errors']}"
            raise Exception(msg)

    async def close(self):
        self._sleeps.cancel()
        await self.session.close()
        del self.session

    async def ping(self):
        await self.make_request(graphql_query=PING_QUERY)


class GraphQLDataSource(BaseDataSource):
    """GraphQL"""

    name = "GraphQL"
    service_type = "graphql"

    def __init__(self, configuration):
        """Setup the connection to the GraphQL instance.

        Args:
            configuration (DataSourceConfiguration): Instance of DataSourceConfiguration class.
        """
        super().__init__(configuration=configuration)
        self.graphql_client = GraphQLClient(configuration=configuration)

    def _set_internal_logger(self):
        self.graphql_client.set_logger(self._logger)

    @classmethod
    def get_default_configuration(cls):
        """Get the default configuration for GraphQL.

        Returns:
            dict: Default configuration.
        """
        return {
            "http_endpoint": {
                "label": "HTTP URL & Endpoint",
                "order": 1,
                "type": "str",
            },
            "http_method": {
                "display": "dropdown",
                "label": "GET/POST",
                "options": [
                    {"label": "GET", "value": "get"},
                    {"label": "POST", "value": "post"},
                ],
                "order": 2,
                "type": "str",
                "value": "POST",
            },
            "authentication_method": {
                "display": "dropdown",
                "label": "Authentication Method",
                "options": [
                    {"label": "No Auth", "value": "none"},
                    {"label": "Basic Auth", "value": "basic"},
                    {"label": "Bearer Token", "value": "bearer"},
                ],
                "order": 3,
                "type": "str",
                "value": "none",
            },
            "username": {
                "depends_on": [{"field": "authentication_method", "value": "basic"}],
                "label": "Username",
                "order": 4,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "authentication_method", "value": "basic"}],
                "label": "Password",
                "order": 5,
                "sensitive": True,
                "type": "str",
            },
            "token": {
                "depends_on": [{"field": "authentication_method", "value": "bearer"}],
                "label": "Bearer Token",
                "order": 6,
                "sensitive": True,
                "type": "str",
            },
            "graphql_query": {
                "display": "textarea",
                "label": "GraphQL Body",
                "order": 7,
                "type": "str",
            },
            "graphql_variables": {
                "depends_on": [{"field": "http_method", "value": "post"}],
                "display": "textarea",
                "label": "Graphql Variables",
                "order": 8,
                "type": "str",
                "required": False,
            },
            "graphql_object_list": {
                "label": "GraphQL Objects List",
                "order": 9,
                "tooltip": "Specifies which GraphQL objects should be indexed as individual documents. This allows finer control over indexing, ensuring only relevant data sections from the GraphQL response are stored as separate documents. Use '.' to provide full path of the object from the root of the response. For example 'organization.users.nodes'",
                "type": "list",
            },
            "headers": {
                "label": "Headers",
                "order": 10,
                "type": "str",
                "required": False,
            },
            "pagination_model": {
                "display": "dropdown",
                "label": "Pagination model",
                "options": [
                    {"label": "No pagination", "value": "no_pagination"},
                    {"label": "Cursor-based pagination", "value": "cursor_pagination"},
                ],
                "order": 11,
                "tooltip": "Cursor based pagination requires 'pageInfo' field along with argument 'after' for objects mentioned in 'GraphQL Objects List'. It also requires variable for 'after' argument.",
                "type": "str",
                "value": "none",
            },
            "pagination_key": {
                "depends_on": [
                    {"field": "pagination_model", "value": "cursor_pagination"}
                ],
                "label": "Pagination key",
                "order": 12,
                "tooltip": "Specifies which GraphQL object is used for pagination. Use '.' to provide full path of the object from the root of the response. For example 'organization.users'",
                "type": "str",
            },
            "connection_timeout": {
                "default_value": 300,
                "display": "numeric",
                "label": "Connection Timeout",
                "order": 13,
                "required": False,
                "type": "int",
                "ui_restrictions": ["advanced"],
            },
        }

    def is_query(self, graphql_query):
        try:
            ast = parse(graphql_query)  # pyright: ignore
            for definition in ast.definitions:  # pyright: ignore
                if (
                    hasattr(definition, "operation")
                    and definition.operation.value != "query"
                ):
                    return False
            return True
        except Exception as e:
            self._logger.error(f"Failed to parse GraphQL query. Error: {e}")
            return False

    def validate_endpoints(self):
        if re.match(URL_REGEX, self.graphql_client.url):
            return True
        return False

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured repositories are accessible or not and scope of the token
        """
        await super().validate_config()

        if not self.validate_endpoints():
            msg = "HTTP URL & Endpoint are not structured."
            raise ConfigurableFieldValueError(msg)

        if not self.is_query(graphql_query=self.graphql_client.graphql_query):
            msg = "Configured Query is not supported by the connector."
            raise ConfigurableFieldValueError(msg)

        headers = self.graphql_client.configuration["headers"]
        graphql_variables = self.graphql_client.configuration["graphql_variables"]
        if headers:
            try:
                self.graphql_client.headers = json.loads(headers)
            except Exception as exception:
                msg = f"Error while processing configured GraphQL headers. Exception: {exception}"
                raise ConfigurableFieldValueError(msg) from exception

        if graphql_variables:
            try:
                self.graphql_client.variables = json.loads(graphql_variables)
            except Exception as exception:
                msg = f"Error while processing configured GraphQL variables. Exception: {exception}"
                raise ConfigurableFieldValueError(msg) from exception

    async def close(self):
        await self.graphql_client.close()

    async def ping(self):
        try:
            await self.graphql_client.ping()
            self._logger.debug("Successfully connected to GraphQL Instance.")
        except Exception:
            self._logger.exception("Error while connecting to GraphQL Instance.")
            raise

    def yield_dict(self, documents):
        if isinstance(documents, dict):
            yield documents
        elif isinstance(documents, list):
            for document in documents:
                if isinstance(document, dict):
                    yield document

    async def fetch_data(self, graphql_query):
        if self.graphql_client.pagination_model == "no_pagination":
            data = await self.graphql_client.make_request(graphql_query=graphql_query)
            for documents in self.graphql_client.extract_graphql_data_items(data=data):
                for document in self.yield_dict(documents):
                    yield document
        else:
            async for data in self.graphql_client.paginated_call(
                graphql_query=graphql_query
            ):
                for document in self.yield_dict(data):
                    yield document

    async def get_docs(self, filtering=None):
        """Executes the logic to fetch GraphQL response in async manner.

        Args:
            filtering (filtering, None): Filtering Rules. Defaults to None.

        Yields:
            dict: Documents from GraphQL.
        """
        doc_id = 1
        async for doc in self.fetch_data(
            graphql_query=self.graphql_client.graphql_query
        ):
            doc = deepcopy(doc)
            doc["_id"] = doc.get("id") or hash_id(str(doc_id))
            doc["_timestamp"] = iso_utc()
            doc_id += 1
            yield doc, None
