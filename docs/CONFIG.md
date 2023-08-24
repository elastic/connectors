# Configuration

Configuration lives in [config.yml](../config.yml).

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
