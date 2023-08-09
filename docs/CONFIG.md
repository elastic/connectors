# Configuration

Configuration lives in [config.yml](../config.yml).

- `elasticsearch`: Elasticsearch connection configurations.
  - `host`: The host of the Elasticsearch deployment.
    Default is `"http://localhost:9200"`.
  - `api_key`: The API key for Elasticsearch connection.
    Using `api_key` is recommended instead of `username`/`password`.
    No default.
  - `username`: The username for the Elasticsearch connection.
    Using `username` requires `password` to also be configured.
    However, `api_key` is the recommended configuration choice.
    Default is `"elastic"`
  - `password`: The password for the Elasticsearch connection.
    Using `password` requires `username` to also be configured.
    However, `api_key` is the recommended configuration choice.
    Default is `"changeme"`
  - `ssl`: Whether SSL is used for the Elasticsearch connection.
    Default is `True`
  - `ca_certs`: Path to a CA bundle.
    Default is null.
  - `bulk`: Options for the Bulk API calls behavior - all options can be
    overridden by each source class
    - `display_every`: The number of docs between each counters display.
      Default is `100`.
    - `queue_max_size`: The max size of the bulk queue.
      Default is `1024`.
    - `queue_max_mem_size`: The max size in MB of the bulk queue.
      When it's reached, the next put operation waits for the queue size to get under that limit.
      Default is `25`.
    - `chunk_max_mem_size`: The max size in MB of a bulk request.
      When the next request being prepared reaches that size, the query is emitted even if `chunk_size` is not yet reached.
      Default is `5`.
    - `chunk_size`: The max size of the bulk operation to Elasticsearch.
      Default is `500`.
    - `max_concurrency`: Maximum number of concurrent bulk requests.
      Default is `5`.
    - `concurrent_downloads`: Maximum number of concurrent downloads in the backend.
      Default is `10`.
  - `retry_on_timeout`: Whether to retry on request timeout.
    Default is `True`.
  - `request_timeout`: The request timeout to be passed to transport in options.
    Default is `120`.
  - `max_wait_duration`: The maximum wait duration (in seconds) for the Elasticsearch connection.
    Default is `60`.
  - `initial_backoff_duration`: The initial backoff duration (in seconds).
    Defaults is `1`.
  - `backoff_multiplier`: The backoff multiplier.
    Default is `2`.
  - `log_level`: Elasticsearch log level.
    Default is `INFO`.
- `service`: Connector service related configurations.
  - `idling`: The interval (in seconds) to poll connectors from Elasticsearch.
    Default is `30`.
  - `heartbeat`: The interval (in seconds) to send a new heartbeat for a connector.
    Default is `300`.
  - `preflight_max_attempts`: The maximum number of retries for pre-flight check.
    Default is `10`.
  - `preflight_idle`: The number of seconds to wait between each pre-flight check.
    Default is `30`.
  - `max_errors`: The maximum number of errors allowed in one event loop.
    Default is `20`.
  - `max_errors_span`: The number of seconds to reset `max_errors` count.
    Default is `600`.
  - `max_concurrent_syncs`: (Deprecated. Use `max_concurrent_content_syncs`) The maximum number of concurrent content syncs.
    Default is `1`. 
  - `max_concurrent_content_syncs`: The maximum number of concurrent content syncs.
    Replaces `max_concurrent_syncs`.
    Default is `1`.
  - `max_concurrent_access_control_syncs`: The maximum number of concurrent access control syncs.
    Default is `1`.
  - `job_cleanup_interval`: The interval (in seconds) to run job cleanup task.
    Default is `300`
  - `log_level`: Connector service log level.
    Defaults is `INFO`.
- `extraction_service`: Local extraction service-related configurations.
  These configurations are optional and are not included by default.
  The presence of these configurations enables [local content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html#connectors-content-extraction-local).
  - `host`: The host of the local extraction service.
  - `timeout`: Request timeout for local extraction service requests, in seconds.
    Default is `30`.
  - `use_file_pointers`: Whether or not to use file pointers for local extraction.
    Default is `False`.
  - `stream_chunk_size`: The size (in bytes) that files are chunked to for streaming when sending a file to the local extraction service.
    Only applicable if `use_file_pointers` is `false`.
    Default is `65536`.
  - `shared_volume_dir`: The location for files to be extracted from.
    Only applicable if `use_file_pointers` is `true`.
    Default is `/app/files`.
- `connector_id`: (Deprecated. Configure the connector client in `connectors`) The ID of the connector.
- `service_type`: (Deprecated. Configure the connector client in `connectors`) The service type of the connector.
- `connectors`: A list of connector clients/customized connectors configuration, including:
  - `connector_id`: The ID of the connector.
    Required. No default.
  - `service_type`: The service type of the connector.
    Required. No default.
  - `api_key`: The API key to write data into the content index.
- `sources`: A mapping/dictionary between service type and [Fully Qualified Name
(FQN)](https://en.wikipedia.org/wiki/Fully_qualified_name). E.g. `mongodb: connectors.sources.mongo:MongoDataSource`.
  Default includes all tech preview, beta, and GA connectors in this repository.

## Run the connector service on Elastic Cloud

When you have an Enterprise Search deployment on Elastic Cloud post 8.5.0, the connector service is automatically deployed.
The connector service runs in native mode on Elastic Cloud, and the Elasticsearch connection configurations (i.e. `host`, `username`, `password`) are automatically configured.
A special Cloud user (`cloud-internal-enterprise_search-server`) is used for the Elasticsearch connection.
This user has proper privileges on the connector index (`.elastic-connectors`), the connector job index (`.elastic-connectors-sync-jobs`) and the connector content indices (`search-*`).

If you wish to connect more connector types than are natively available on Elastic Cloud, you will need to run the connector service on-prem, as described below.

## Run the connector service on-prem

Any converted native connector, net-new connector, or customized/modified connector must be run as a connector client, on premises.  

1. Go to Kibana, _Enterprise Search_ > _Create an Elasticsearch index_. Select the service type of connector you wish to use.
2. Create an API key to work with the connector. It should be done using the `Generate API key` button under `Configuration` tab.
3. Configure your connector service application. You need to configure the following fields, and leave the rest as default.
   1. `elasticsearch.host`: Configure this to the Elasticsearch endpoint.
   2. `elasticsearch.api_key`: Configure the API key generated in step 2. If you run multiple connector clients, or customized connectors in one connector service, you can configure the API key of any connector here. This is recommended over utilizing `elasticsearch.username/password` so that access can be automatically and narrowly scoped.
   3. `connectors`: Configure per-connector configuration here. It should at least contain `connector_id` (You can find the `connector_id` in step 3 `Deploy a connector` under `Configuration` tab in Kibana) and `service_type`. You can also configure the `api_key` if the global `elasticsearch` configuration does not have access to the content index of this connector. One example:
   
   ```yaml
   connectors:
     -
       connector_id: <connector_id>
       service_type: <service_type>
       api_key: <api_key>
    ```
4. Run the connector service application with
    ```shell
    make run
    ```
