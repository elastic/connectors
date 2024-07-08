from application.base import BaseDataSource


class GraphQLDataSource(GraphQLDataSource):
    """
    GraphQLDataSource class generated for connecting to the data source.

    Args:

        http_endpoint (str): GraphQL HTTP endpoint

        http_method (str): HTTP method for GraphQL requests

        authentication_method (str): Authentication Method

        username (str): Username

        password (str): Password

        token (str): Bearer Token

        graphql_query (str): GraphQL Body

        graphql_variables (str): Graphql Variables

        graphql_object_to_id_map (str): GraphQL Objects to ID mapping
            - Specifies which GraphQL objects should be indexed as individual documents. This allows finer control over indexing, ensuring only relevant data sections from the GraphQL response are stored as separate documents. Use a JSON with key as the GraphQL object name and value as string field within the document, with the requirement that each document must have a distinct value for this field. Use '.' to provide full path of the object from the root of the response. For example {'organization.users.nodes': 'id'}

        headers (str): Headers

        pagination_model (str): Pagination model
            - For cursor-based pagination, add 'pageInfo' and an 'after' argument variable in your query at the desired node (Pagination key). Use 'after' query argument with a variable to iterate through pages. Detailed examples and setup instructions are available in the docs.

        pagination_key (str): Pagination key
            - Specifies which GraphQL object is used for pagination. Use '.' to provide full path of the object from the root of the response. For example 'organization.users'

        connection_timeout (int): Connection Timeout

    """

    def __init__(
        self,
        http_endpoint=None,
        http_method="post",
        authentication_method="none",
        username=None,
        password=None,
        token=None,
        graphql_query=None,
        graphql_variables=None,
        graphql_object_to_id_map=None,
        headers=None,
        pagination_model="no_pagination",
        pagination_key=None,
        connection_timeout=None,
    ):
        configuration = self.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args[key] is not None:
                configuration[key]["value"] = args[key]

        # Check if all fields marked as 'required' in config are present with values, if not raise an exception
        for key, value in configuration.items():
            if value["value"] is None and value.get("required", True):
                raise ValueError(f"Missing required configuration field: {key}")

        super().__init__(configuration)

        self.http_endpoint = http_endpoint
        self.http_method = http_method
        self.authentication_method = authentication_method
        self.username = username
        self.password = password
        self.token = token
        self.graphql_query = graphql_query
        self.graphql_variables = graphql_variables
        self.graphql_object_to_id_map = graphql_object_to_id_map
        self.headers = headers
        self.pagination_model = pagination_model
        self.pagination_key = pagination_key
        self.connection_timeout = connection_timeout
