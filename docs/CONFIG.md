# Configuration

Configuration lives in [config.yml](../config.yml).

- `elasticsearch`: Elasticsearch connection configurations.
  - `host`: The host of the Elasticsearch deployment.
  - `username`: The username for the Elasticsearch connection.
  - `password`: The password for the Elasticsearch connection.
  - `api_key`: The API key for Elasticsearch connection. You can't set `api_key` when basic auth is used.
  - `ssl`: Whether SSL is used for the Elasticsearch connection.
  - `ca_certs`: Path to a CA bundle.
  - `bulk`: Options for the Bulk API calls behavior - all options can be
    overriden by each source class
    - `display_every`: The number of docs between each counters display. Defaults to 100.
    - `queue_max_size`: The max size of the bulk queue. Defaults to 1024.
    - `queue_max_mem_size`: The max size in MB of the bulk queue. When it's reached, the next put
       operation waits for the queue size to get under that limit. Defaults to 25.
    - `chunk_max_mem_size`: The max size in MB of a bulk request. When the next request being
       prepared reaches that size, the query is emitted even if `chunk_size` is not yet reached. Defaults to 5.
    - `chunk_size`: The max size of the bulk operation to Elasticsearch. Defaults to 500.
    - `max_concurrency`: Maximum number of concurrent bulk requests. Defaults to 5.
    - `concurrent_downloads`: Maximum number of concurrent downloads in the backend. Default to 10.
  - `retry_on_timeout`: Whether to retry on request timeout. Defaults to `true`.
  - `request_timeout`: The request timeout to be passed to transport in options. Defaults to 120.
  - `max_wait_duration`: The maximum wait duration (in seconds) for the Elasticsearch connection. Defaults to 60.
  - `initial_backoff_duration`: The initial backoff duration (in seconds). Defaults to 5.
  - `backoff_multiplier`: The backoff multiplier. Defaults to 2.
  - `log_level`: Elasticsearch log level. Defaults to `INFO`.
- `service`: Connector service related configurations.
  - `idling`: The interval (in seconds) to poll connectors from Elasticsearch.
  - `heartbeat`: The interval (in seconds) to send a new heartbeat for a connector.
  - `preflight_max_attempts`: The maximum number of retries for pre-flight check. Defaults to 10.
  - `preflight_idle`: The number of seconds to wait between each pre-flight check. Defaults to 30.
  - `max_errors`: The maximum number of errors allowed in one event loop.
  - `max_errors_span`: The number of seconds to reset `max_errors` count.
  - `max_concurrent_syncs`: The maximum number of concurrent syncs. Defaults to 1.
  - `job_cleanup_interval`: The interval (in seconds) to run job cleanup task.
  - `log_level`: Connector service log level. Defaults to `INFO`.
- `native_service_types`: An array of supported native connectors (in service type).
- `connector_id`: The ID of the custom connector.
- `service_type` The service type of the custom connector.
- `sources`: A mapping/dictionary between service type and [Fully Qualified Name
(FQN)](https://en.wikipedia.org/wiki/Fully_qualified_name). E.g. `mongodb: connectors.sources.mongo:MongoDataSource`.

## Run the connector service on Elastic Cloud

When you have an Enterprise Search deployment on Elastic Cloud post 8.5.0, the connector service is automatically deployed. The connector service can only run in native mode on Elastic Cloud (i.e. it will only sync data for connectors with service type listed in `native_service_types`), and the Elasticsearch connection configurations (i.e. `host`, `username`, `password`) will be overridden, and a special Cloud user `cloud-internal-enterprise_search-server` will be used for Elasticsearch connection, which will have proper privilege on the connector index (`.elastic-connectors`), the connector job index (`.elastic-connectors-sync-jobs`) and the connector content indices (`search-*`).

## Run the connector service on-prem

### Run the connector service in native mode

1. Make sure the service types of supported native connectors are configured in `native_service_types`.
2. Configure the Elasticsearch connection, with basic auth (`username` and `password`), or with API key (generated via _Stack Management_ > _Security_ > _API keys_ > _Create API key_). Make sure the user or the API key has at least the privileges to `manage`, `read` and `write` the connector index (`.elastic-connectors`), the connector job index (`.elastic-connectors-sync-jobs`) and the connector content indices (`search-*`).
3. Run the connector service with
    ```shell
    make run
    ```

### Run the connector service for a custom connector

1. Go to Kibana, _Enterprise Search_ > _Create an Elasticsearch index_. Use the `Build a connector` option for an ingestion method to create an index.
2. Create an API key to work with the connector. It should be done using the `Generate API key` button under `Configuration` tab.
3. Configure your connector service application. You need to configure the followings fields, and leave the rest as default.
   1. `elasticsearch.host`: Configure this to the Elasticsearch endpoint.
   2. `elasticsearch.api_key`: Configure the API key generated in step 2. Make sure `elasticsearch.username` is not configured.
   3. `connector_id`: You can find the `connector_id` in step 3 `Deploy a connector` under `Configuration` tab in Kibana.
   4. `service_type`: Configure it to the service type of your new connector.
4. Run the connector service application with
    ```shell
    make run
    ```
