#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import json
from functools import cached_property

import aiohttp
from aiohttp import ClientResponseError
from connectors_sdk.logger import logger
from connectors_sdk.source import ConfigurableFieldValueError
from graphql.language import (
    FieldNode,
    FragmentDefinitionNode,
    FragmentSpreadNode,
    InlineFragmentNode,
    OperationDefinitionNode,
    OperationType,
    parse,
)
from graphql.language.ast import VariableNode

from connectors.sources.graphql.constants import (
    BASIC,
    BEARER,
    CURSOR_PAGINATION,
    GET,
    PING_QUERY,
    RETRIES,
    RETRY_INTERVAL,
)
from connectors.utils import CancellableSleeps, RetryStrategy, retryable


def response_key(field):
    # The response uses the alias of a field when it has one.
    return field.alias.value if field.alias else field.name.value


def selected_fields(selection_sets, fragments):
    """Yield the fields that the given selection sets of one object select.

    The fields of inline fragments and fragment spreads belong to the enclosing
    object in the response, so look through them. Every named fragment is
    expanded at most once per object, which keeps the walk linear in the size
    of the query and stops a fragment that spreads itself.
    """
    expanded = set()
    pending = list(selection_sets)
    while pending:
        for selection in pending.pop(0):
            if isinstance(selection, FieldNode):
                yield selection
            elif isinstance(selection, InlineFragmentNode):
                pending.append(selection.selection_set.selections)
            elif isinstance(selection, FragmentSpreadNode):
                name = selection.name.value
                if name in fragments and name not in expanded:
                    expanded.add(name)
                    pending.append(fragments[name].selection_set.selections)


def fields_at_path(ast, path):
    """Return every selection of the field at a response path of the query.

    Response keys are only unique within their parent object, so the path is
    followed from the root of the first query operation. A field can be
    selected more than once (for example in two fragments); all of its
    selections are returned, as the response merges them.
    """
    fragments = {
        definition.name.value: definition
        for definition in ast.definitions
        if isinstance(definition, FragmentDefinitionNode)
    }
    for definition in ast.definitions:
        if (
            isinstance(definition, OperationDefinitionNode)
            and definition.operation == OperationType.QUERY
        ):
            selection_sets = [definition.selection_set.selections]
            matches = []
            for key in path:
                matches = [
                    field
                    for field in selected_fields(selection_sets, fragments)
                    if response_key(field) == key
                ]
                selection_sets = [
                    field.selection_set.selections
                    for field in matches
                    if field.selection_set
                ]
            return matches, fragments
    return [], fragments


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

    def validate_paginated_query(self, ast):
        """Return the variable that carries the cursor of the paginated field."""
        graphql_object = self.pagination_key.split(".")[-1]
        self._logger.debug(f"Finding pageInfo field in {graphql_object}.")
        fields, fragments = fields_at_path(ast, self.pagination_key.split("."))
        cursor_variables = [
            argument.value.name.value
            for field in fields
            for argument in field.arguments or ()
            if argument.name.value == "after"
            and isinstance(argument.value, VariableNode)
        ]
        page_info_present = any(
            response_key(field) == "pageInfo"
            for field in selected_fields(
                [
                    field.selection_set.selections
                    for field in fields
                    if field.selection_set
                ],
                fragments,
            )
        )
        if not (cursor_variables and page_info_present):
            msg = f"Pagination is enabled but 'pageInfo' not found. Please include 'pageInfo' field inside '{graphql_object}' and 'after' argument in '{graphql_object}'."
            raise ConfigurableFieldValueError(msg)
        return cursor_variables[0]

    async def paginated_call(self, graphql_query):
        if self.pagination_model == CURSOR_PAGINATION:
            ast = parse(graphql_query)  # pyright: ignore
            cursor_variable = self.validate_paginated_query(ast)
            while True:
                self._logger.debug(
                    f"Fetching document with variables: {self.variables}."
                )

                has_new_page = False
                data = await self.make_request(graphql_query)
                (
                    has_next_page,
                    end_cursor,
                    _,
                ) = self.extract_pagination_info(data)

                if has_next_page and end_cursor:
                    self.variables[cursor_variable] = end_cursor
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
        if self.variables:
            # GraphQL over HTTP sends variables on GET as a JSON-encoded query parameter
            params["variables"] = json.dumps(self.variables)
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
