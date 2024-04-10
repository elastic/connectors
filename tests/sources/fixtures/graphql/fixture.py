#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module to handle api calls received from connector.
"""

import base64
import os

from flask import Flask, request

from tests.commons import WeightedFakeProvider

fake_provider = WeightedFakeProvider()

DATA_SIZE = os.environ.get("DATA_SIZE", "medium")

match DATA_SIZE:
    case "small":
        DOC_COUNT = 1800
    case "medium":
        DOC_COUNT = 2500
    case "large":
        DOC_COUNT = 4000


class GraphQLAPI:
    def __init__(self):
        self.app = Flask(__name__)
        self.app.route("/graphql", methods=["POST"])(self.mock_graphql_response)

    def encode_cursor(self, value):
        return base64.b64encode(str(value).encode()).decode()

    def decode_cursor(self, cursor):
        return int(base64.b64decode(cursor.encode()).decode())

    def get_index_metadata(self, variables, data):
        after = variables.get("cursor")
        start_index = self.decode_cursor(after) if after else 0
        end_index = start_index + 100
        subset_nodes = data["nodes"][start_index:end_index]
        return start_index, end_index, subset_nodes

    def mock_graphql_response(self):
        issue_data = {
            "nodes": [
                {
                    "id": f"user_{issue_number}",
                    "first_name": fake_provider.name(),
                    "updatedAt": "2023-04-20T09:32:54Z",
                    "description": fake_provider.get_text(),
                }
                for issue_number in range(DOC_COUNT)
            ],
            "pageInfo": {
                "hasNextPage": False,
                "hasPreviousPage": False,
                "startCursor": "",
                "endCursor": "",
            },
        }

        mock_data = {}
        data = request.get_json()
        query = data.get("query")
        variables = data.get("variables", {})

        query = query.replace(" ", "").replace("\r", "").replace("\n", "")
        if (
            query
            == "query($cursor:String!){sampleData{users(after:$cursor){pageInfo{endCursorhasNextPage}nodes{idfirst_nameupdatedAtdescription}}}}"
        ):
            start_index, end_index, subset_nodes = self.get_index_metadata(
                variables, issue_data
            )
            mock_data["data"] = {
                "sampleData": {
                    "users": {
                        "nodes": subset_nodes,
                        "pageInfo": {
                            "hasNextPage": end_index < len(issue_data["nodes"]),
                            "hasPreviousPage": start_index > 0,
                            "startCursor": self.encode_cursor(start_index + 1),
                            "endCursor": self.encode_cursor(end_index),
                        },
                    }
                }
            }
        else:
            return {"data": {"__schema": {"queryType": {"name": "Query"}}}}

        return mock_data


if __name__ == "__main__":
    GraphQLAPI().app.run(host="0.0.0.0", port=9094)
