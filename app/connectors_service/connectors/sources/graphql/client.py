#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from functools import cached_property

import aiohttp
from aiohttp import ClientResponseError
from connectors_sdk.logger import logger
from connectors_sdk.source import ConfigurableFieldValueError
from graphql.language import parse, visit
from graphql.language.ast import VariableNode
from graphql.language.visitor import Visitor

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
                    self.variables_dict[node.name.value][arg.name.value] = (
                        arg.value.name.value
                    )


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
                    self.variables[visitor.variables_dict[pagination_key]["after"]] = (
                        end_cursor
                    )
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
