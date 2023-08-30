e2e fixtures
------------

Each fixture needs to implement the following:

- create a directory here that matches the service type
- add in it the following files:

  - config.yml
  - fixture.py
  - requirements.txt
  - docker-compose.yml
  - connector.json

config.yml
==========

The config file necessary to run the connector for the ftest.
Specifically, this must set the `connector_id` and `service_type` for the connector.
Other configuration changes are optional.

fixture.py
==========

This file may contain four functions (all optional):

- load -- loads data in the backend
- remove -- removes random data in the backend
- setup -- called before the docker is started
- teardown -- called after the docker has been torn down

requirements.txt
================

pip requirements. Lists all libs needed for `fixture.py` to run

docker-compose.yml
==================

A Docker compose file that needs to run the whole stack:

- Elasticsearch
- Kibana
- Enterprise Search
- Any backend server like MySQL

connector.json
==========

This file should be a JSON representation of the connector document as it would exist in Elastic.
