# Configuration

Configuration lives in [config.yml](../config.yml).

- `elasticsearch`: Elasticsearch connection configurations.
  - `host`: The host of the Elasticsearch deployment.
  - `username`: The username for the Elasticsearch connection.
  - `password`: The password for the Elasticsearch connection.
  - `api_key`: The API key for Elasticsearch connection. You can't set `api_key` when basic auth is used.
  - `ssl`: Whether SSL is used for the Elasticsearch connection.
  - `ca_certs`: Path to a CA bundle.
  - `bulk_queue_max_size`: The max size of the bulk queue. Defaults to 1024.
  - `bulk_chunk_size`: The max size of the bulk operation to Elasticsearch. Defaults to 500.
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
- `native_service_types`: An array of supported native connectors (in service type).
- `connector_id`: The ID of the custom connector.
- `servcie_type` The service type of the custom connector.
- `sources`: A mapping/dictionary between service type and [Fully Qualified Name
(FQN)](https://en.wikipedia.org/wiki/Fully_qualified_name). E.g. `mongodb: connectors.sources.mongo:MongoDataSource`.
