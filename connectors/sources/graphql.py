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
    iso_utc,
    retryable,
)

RETRIES = 3
RETRY_INTERVAL = 2

BASIC = "basic"
BEARER = "bearer"
CURSOR_PAGINATION = "cursor_pagination"
GET = "get"
NO_PAGINATION = "no_pagination"
POST = "post"

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
        self.http_method = self.configuration["http_method"]
        self.authentication_method = self.configuration["authentication_method"]
        self.pagination_model = self.configuration["pagination_model"]
        self.pagination_key = self.configuration["pagination_key"]
        self.graphql_object_to_id_map = {}
        self.valid_objects = []
        self.variables = {}
        self.headers = {}

    def set_logger(self, logger_):
        self._logger = logger_

    @cached_property
    def session(self):
        timeout = aiohttp.ClientTimeout(total=self.configuration["connection_timeout"])
        if self.authentication_method == BEARER:
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
        elif self.authentication_method == BASIC:
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
        """Returns sub objects from the response based on graphql_object_to_id_map

        Args:
            data (dict): data to extract

        Yields:
            dictionary/list: Documents from the response
        """
        for keys, field_id in self.graphql_object_to_id_map.items():
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
                    current_level_data["_id"] = current_level_data.get(field_id)
                    yield current_level_data

            if isinstance(current_level_data, list):
                for doc in current_level_data:
                    doc["_id"] = doc.get(field_id)
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
            msg = "Pagination is enabled but the query is missing 'pageInfo'. Please include 'pageInfo { hasNextPage endCursor }' in the query to support pagination."
            raise ConfigurableFieldValueError(msg)

    def validate_paginated_query(self, graphql_query, visitor):
        graphql_object = self.pagination_key.split(".")[-1]
        self._logger.debug(f"Finding pageInfo field in {graphql_object}.")
        if not (
            {"after"}.issubset(set(visitor.fields_dict.get(graphql_object, [])))
            and "pageInfo" in visitor.fields_dict.keys()
        ):
            msg = f"Pagination is enabled but 'pageInfo' not found. Please include 'pageInfo' field inside '{graphql_object}' and 'after' argument in '{graphql_object}'."
            raise ConfigurableFieldValueError(msg)

    async def paginated_call(self, graphql_query):
        if self.pagination_model == CURSOR_PAGINATION:
            ast = parse(graphql_query)  # pyright: ignore
            visitor = FieldVisitor()
            visit(ast, visitor)  # pyright: ignore

            self.validate_paginated_query(graphql_query, visitor)
            while True:
                self._logger.debug(
                    f"Fetching document with variables: {self.variables}."
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
            if self.http_method == GET:
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
                "label": "GraphQL HTTP endpoint",
                "order": 1,
                "type": "str",
            },
            "http_method": {
                "display": "dropdown",
                "label": "HTTP method for GraphQL requests",
                "options": [
                    {"label": "GET", "value": GET},
                    {"label": "POST", "value": POST},
                ],
                "order": 2,
                "type": "str",
                "value": POST,
            },
            "authentication_method": {
                "display": "dropdown",
                "label": "Authentication Method",
                "options": [
                    {"label": "No Auth", "value": "none"},
                    {"label": "Basic Auth", "value": BASIC},
                    {"label": "Bearer Token", "value": BEARER},
                ],
                "order": 3,
                "type": "str",
                "value": "none",
            },
            "username": {
                "depends_on": [{"field": "authentication_method", "value": BASIC}],
                "label": "Username",
                "order": 4,
                "type": "str",
            },
            "password": {
                "depends_on": [{"field": "authentication_method", "value": BASIC}],
                "label": "Password",
                "order": 5,
                "sensitive": True,
                "type": "str",
            },
            "token": {
                "depends_on": [{"field": "authentication_method", "value": BEARER}],
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
                "depends_on": [{"field": "http_method", "value": POST}],
                "display": "textarea",
                "label": "Graphql Variables",
                "order": 8,
                "type": "str",
                "required": False,
            },
            "graphql_object_to_id_map": {
                "display": "textarea",
                "label": "GraphQL Objects to ID mapping",
                "order": 9,
                "tooltip": "Specifies which GraphQL objects should be indexed as individual documents. This allows finer control over indexing, ensuring only relevant data sections from the GraphQL response are stored as separate documents. Use a JSON with key as the GraphQL object name and value as string field within the document, with the requirement that each document must have a distinct value for this field. Use '.' to provide full path of the object from the root of the response. For example {'organization.users.nodes': 'id'}",
                "type": "str",
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
                    {"label": "No pagination", "value": NO_PAGINATION},
                    {"label": "Cursor-based pagination", "value": CURSOR_PAGINATION},
                ],
                "order": 11,
                "tooltip": "For cursor-based pagination, add 'pageInfo' and an 'after' argument variable in your query at the desired node (Pagination key). Use 'after' query argument with a variable to iterate through pages. Detailed examples and setup instructions are available in the docs.",
                "type": "str",
                "value": NO_PAGINATION,
            },
            "pagination_key": {
                "depends_on": [
                    {"field": "pagination_model", "value": CURSOR_PAGINATION}
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

    def is_query(self, ast):
        for definition in ast.definitions:  # pyright: ignore
            if (
                hasattr(definition, "operation")
                and definition.operation.value != "query"
            ):
                return False
        return True

    def validate_endpoints(self):
        if re.match(URL_REGEX, self.graphql_client.url):
            return True
        return False

    def check_field_existence(
        self, ast, field_path, graphql_field_id=None, check_id=False
    ):
        def traverse(selections, path):
            for selection in selections:
                if selection.name.value == path[0]:
                    if len(path) == 1:
                        if check_id and selection.selection_set:
                            for selection_node in selection.selection_set.selections:
                                if selection_node.name.value == graphql_field_id:
                                    return True, True
                        return True, False
                    if selection.selection_set:
                        return traverse(
                            selections=selection.selection_set.selections, path=path[1:]
                        )
            return False, False

        field_path = field_path.split(".")
        for definition in ast.definitions:
            if definition.operation.value == "query":
                return traverse(definition.selection_set.selections, field_path)

        return False, False

    async def validate_config(self):
        """Validates whether user input is empty or not for configuration fields
        Also validate, if user configured repositories are accessible or not and scope of the token
        """
        await super().validate_config()

        if not self.validate_endpoints():
            msg = "GraphQL HTTP endpoint is not a valid URL."
            raise ConfigurableFieldValueError(msg)

        try:
            ast = parse(self.graphql_client.graphql_query)  # pyright: ignore
        except Exception as exception:
            msg = f"Failed to parse GraphQL query. Error: {exception}"
            raise ConfigurableFieldValueError(msg) from exception

        if not self.is_query(ast=ast):
            msg = "The configured query is either invalid or a mutation which is not supported by the connector."
            raise ConfigurableFieldValueError(msg)

        headers = self.graphql_client.configuration["headers"]
        graphql_variables = self.graphql_client.configuration["graphql_variables"]
        graphql_object_to_id_map = self.graphql_client.configuration[
            "graphql_object_to_id_map"
        ]
        invalid_json_msg = (
            "GraphQL {field} must be in valid JSON format. Exception: {exception}"
        )

        if headers:
            try:
                self.graphql_client.headers = json.loads(headers)
            except Exception as exception:
                msg = invalid_json_msg.format(field="Headers", exception=exception)
                raise ConfigurableFieldValueError(msg) from exception

        if graphql_variables:
            try:
                self.graphql_client.variables = json.loads(graphql_variables)
            except Exception as exception:
                msg = invalid_json_msg.format(field="Variables", exception=exception)
                raise ConfigurableFieldValueError(msg) from exception

        if graphql_object_to_id_map:
            try:
                self.graphql_client.graphql_object_to_id_map = json.loads(
                    graphql_object_to_id_map
                )
            except Exception as exception:
                msg = invalid_json_msg.format(
                    field="Objects to ID mapping", exception=exception
                )
                raise ConfigurableFieldValueError(msg) from exception
        else:
            msg = "GraphQL Objects to ID mapping field is not configured."
            raise ConfigurableFieldValueError(msg)

        for (
            graphql_object,
            graphql_field_id,
        ) in self.graphql_client.graphql_object_to_id_map.items():
            object_present, id_present = self.check_field_existence(
                ast=ast,
                field_path=graphql_object,
                graphql_field_id=graphql_field_id,
                check_id=True,
            )
            if not object_present:
                msg = f"{graphql_object} is not present in the query."
                raise ConfigurableFieldValueError(msg)
            if not id_present:
                msg = f"{graphql_field_id} is not present in the query."
                raise ConfigurableFieldValueError(msg)

        if self.graphql_client.pagination_model == CURSOR_PAGINATION:
            object_present, _ = self.check_field_existence(
                ast=ast, field_path=self.graphql_client.pagination_key
            )
            if not object_present:
                msg = (
                    f"{self.graphql_client.pagination_key} is not present in the query."
                )
                raise ConfigurableFieldValueError(msg)

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
        if self.graphql_client.pagination_model == NO_PAGINATION:
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
        async for doc in self.fetch_data(
            graphql_query=self.graphql_client.graphql_query
        ):
            doc = deepcopy(doc)
            if doc_id := doc.get("_id"):
                if isinstance(doc_id, str):
                    doc["_timestamp"] = iso_utc()
                    yield doc, None
                else:
                    msg = f"{doc_id} is not a string."
                    raise ConfigurableFieldValueError(msg)
            else:
                self._logger.warning(
                    f"Skipping {doc}, Because connector does not find configured unique field."
                )
