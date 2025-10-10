#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""GraphQL source module responsible to fetch documents based on GraphQL Query."""

import json
import re
from copy import deepcopy

from connectors.sources.graphql.client import GraphQLClient
from connectors.sources.graphql.constants import BASIC, BEARER, CURSOR_PAGINATION, GET, NO_PAGINATION, POST, URL_REGEX
from connectors_sdk.source import BaseDataSource, ConfigurableFieldValueError
from connectors_sdk.utils import (
    iso_utc,
)
from graphql import parse


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
