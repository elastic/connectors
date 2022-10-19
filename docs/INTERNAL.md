# Elastic Internal Documentation

### Testing locally with Enterprise Search and Kibana

##### Setup
* clone [kibana](https://github.com/elastic/kibana)
  * `cd` into your kibana checkout
  * install kibana dependencies with:
    ```shell
    nvm use && yarn kbn clean && yarn kbn bootstrap
    ```
* clone [ent-search](https://github.com/elastic/ent-search/)
  * follow the ent-search [setup steps](https://github.com/elastic/ent-search/#set-up)

##### Start Elasticsearch
* `cd` into your kibana checkout
* start elasticsearch with:
  ```shell
  nvm use && yarn es snapshot -E xpack.security.authc.api_key.enabled=true
  ```

##### Start Kibana
* `cd` into your kibana checkout
* start kibana with:
  ```shell
  nvm use && yarn start --no-base-path
  ```

##### Start Enterprise Search
* `cd` into your ent-search checkout
* start Enterprise Search with:
  ```shell
  script/togo/development start
  ```

##### Start Connectors
* `cd` into your connectors checkout
* run `make install` to get the latest dependencies
* run `make run` to start Connectors.
