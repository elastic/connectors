# GraphQL Connector

The Elastic GraphQL connector connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main). View the [source code for this connector](https://github.com/elastic/connectors/blob/main/connectors/sources/graphql.py).

## Availability and prerequisites

This connector is available as a self-managed connector client.
To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Technical preview features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, select the **GraphQL** tile when creating a new connector under **Search -> Connectors**.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Configuration

### Configure GraphQL connector

Note the following configuration fields:

#### `http_endpoint`  (required)

Provide base URL of the source with endpoint. Example:

 `https://api.xyz.com/graphql`

#### `http_method`  (required)

Select method from `GET` and `POST`.

#### `authentication_method`  (required)

Select authentication method from `No Auth`, `Basic Auth`, and `Bearer Token`.

#### `username` (required in case of `Basic Auth`)

In case of `Basic Auth` authentication method, provides username to connect with the source.

#### `password` (required in case of `Basic Auth`)

In case of `Basic Auth` authentication method, provides password to connect with the source.

#### `token` (required in case of `Bearer Token`)

In case of `Bearer token` authentication method, provides token to connect with the source.

#### `graphql_query` (required)

This query will be used to fetch data from the source. The query can contain variables which can be provided in the `graphql_variables` field. The connector will substitute the variables in the query with values from `graphql_variables` and make a GraphQL query to the source. Example:

```
query getUser($id: ID!) { 
    user(id: $id) {
        name 
        email 
    }
}
```

#### `graphql_variables`

A json of key value pairs of variables used in the GraphQL query. The connector will substitute the variables in the query with the values provided here and make a GraphQL query to the source.

Example: If the GraphQL query is `query getUser($id: ID!) { user(id: $id) { name } }` and the value of `graphql_variables` is `{"id": "123"}`, the final query will be `query getUser { user(id: "123") { name } }` and the connector will make a GraphQL query to the source with the modified query.

#### `graphql_object_list` (required)

A JSON mapping between GraphQL response objects to index and their ID fields. The connector will fetch data for each object (JSON key) and use the provided ID field (JSON value) to index the object into Elasticsearch. The connector will index all fields for each object specified in the mapping. Use a dot `(.)` notation to specify the full path from the root of the GraphQL response to the desired object.

Example: 

If the GraphQL query is `query getUser { organization { users{ user_id name email} } }` which is fetching all available users from the source and we want to index every user as separate document in that case configure this field as below.
```
{
    "organization.users": "user_id"
}
```
Here user_id is unique in all the user document so configure user_id as a value to `organization.users`.

Note: The path provided in this field should only contain Json Objects and not lists.

#### `headers`

Json of custom headers to be sent with each GraphQL request. Example:

```
{
    "content-type": "Application/json"
}
```

#### `pagination_model` (required)

This field specifies the pagination model to be used by the connector. The connector supports `No pagination` and `Cursor-based pagination` pagination models. For cursor-based pagination, add `pageInfo {endCursor hasNextPage}` and an `after` argument variable in your query at the desired node (`Pagination key`). Use `after` query argument with a variable to iterate through pages. Detailed examples and setup instructions are available in the docs. The default value for this field is `No pagination`. Example:

For `Cursor-based pagination`, the query should look like below example:
```
query getUsers($cursor: String!) {
    sampleData { 
        users(after: $cursor) {
            pageInfo {
                endCursor
                hasNextPage
            }
            nodes {
                first_name
                last_name
                address
            } 
        } 
    } 
}
```
and the value of `pagination_key` is `sampleData.users` so, it must contain `pageInfo {endCursor hasNextPage}` and `after` argument with a variable in case of `Cursor-based pagination`.

#### `pagination_key` (required)

Specifies which GraphQL object is used for pagination. Use `.` to provide full path of the object from the root of the response. Example:

- `organization.users`

#### `connection_timeout`

The `connection_timeout` specifies the maximum time in seconds to wait for a response from the GraphQL source. Default value is 30 seconds.

## Documents and syncs

The connector syncs the objects and entities based on GraphQL Query and GraphQL Object List.

### Sync types
[Full syncs](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sync-types.html#connectors-sync-types-full) are supported by default for all connectors.

This connector currently does not support [incremental syncs](https://www.elastic.co/guide/en/enterprise-search/current/connectors-sync-types.html#connectors-sync-types-incremental).

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced sync rules are not available for this connector in the present version.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for GraphQL connector, run the following command:

```shell
$ make ftest NAME=graphql
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a GraphQL source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock GraphQL source using the docker image.

## Known issues

- Every Document will be updated in every sync.
- In case of same field name and different types across different objects, the connector might raise a mapping parser exception.
- Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

