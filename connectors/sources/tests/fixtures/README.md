e2e fixtures
------------

Each fixture needs to implement the following:

- create a directory here that matches the service type
- add in it the following files:

  - fixture.py
  - requirements.txt
  - docker-compose.yml


fixture.py
==========

This file must contain two functions:

- load -- loads data in the backend
- remove -- removes random data in the backend

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


