# Redis Connector

The [Elastic Redis connector](../connectors/sources/redis.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Redis** tile from the connectors list or **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Configuration

### Configure Redis connector

The following configuration fields are required:

#### `host`  (required)

The IP of your Redis server/cloud. Example:

- `127.0.0.1`
- `redis-12345.us-east-1.ec2.cloud.redislabs.com`

#### `port`  (required)

where the Redis server/cloud instance is hosted. Example:

- `6379`

#### `username`  (optional)

Username for your Redis server/cloud. Example:

- `default`

#### `password`  (optional)

Password for your Redis server/cloud. Example:

- `changeme`

#### `database`  (required)

List of database index for your Redis server/cloud. * will fetch data from all databases. Example:

- `0,1,2`
- `*`

#### `ssl_enabled`

Toggle to use the SSL/TLS enabled option. Disabled by default.

#### `mutual_tls_enabled`

Toggle to use the secure mutual SSL/TLS enabled option. Ensure that your Redis deployment supports mutual SSL/TLS connections. Disabled by default.

#### `tls_certfile`

Specifies the certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Redis instance. This field depends on **mutual_tls_enabled**

#### `tls_keyfile`

Specifies the client private key. The value of the key is used to validate the connection in Redis instance. This field depends on **mutual_tls_enabled**

*NOTE*:

- This field is ignored when using advanced sync rules.

## Documents and syncs

The connector syncs the following objects and entities:
- KEYS and VALUES of every database index


*NOTE*:
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced sync rules are defined through a source-specific DSL JSON snippet.

Use advanced sync rules to filter data to be fetched from Redis server/cloud. They take the following parameters:

1. `database`:  Specify the Redis database index as an integer value.
2. `key_pattern`: 2. `key_pattern`: Pattern for finding keys in Redis.
3. `type`: Key type for the Redis.
Supported values: 
 - HASH
 - LIST
 - SET
 - STREAM
 - STRING
 - ZSET

*NOTE*:

Provide at least one of the following: `key_pattern` or `type`, or both.

### Advanced sync rules examples

#### **Fetch database records where keys start with `alpha`**

```json
[
  {
    "database": 0,
    "key_pattern": "alpha*"
  }
]
```

#### Fetch database records with exact match by specifying the full key name.

```json
[
  {
    "database": 0,
    "key_pattern": "alpha"
  }
]
```

#### **Fetch database records where keys start with `test1`, `test2` or `test3`**

```json
[
  {
    "database": 0,
    "key_pattern": "test[123]"
  }
]

```

#### **Exclude database records where keys start with `test1`, `test2` or `test3`**

```json
[
  {
    "database": 0,
    "key_pattern": "test[^123]"
  }
]
```

#### Fetch all database records.

```json
[
  {
    "database": 0,
    "key_pattern": "*"
  }
]
```

#### Fetch all database records where type is SET.

```json
[
  {
    "database": 0,
    "key_pattern": "*",
    "type": "SET"
  }
]
```

#### Fetch database records where type is SET.

```json
[
  {
    "database": 0,
    "type": "SET"
  }
]
```

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Redis server/cloud connector, run the following command:

```shell
$ make ftest NAME=redis
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `SMALL`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Redis server/cloud source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Redis server/cloud source using the docker image.

## Known issues

- The last modified time is unavailable when retrieving keys/values from the Redis database. As a result, **all objects** are indexed each time an advanced sync rule query is executed.

Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).