How to contribute connectors
============================


Checklist
:::::::::


If you want to add a new connector source, you need to:

1. add a module or a directory in `connectors/sources <connectors/sources>`_
2. implement a class that implements all methods described in `connectors.source.BaseDataSource`
3. add a unit test in `connectors/sources/tests <connectors/sources/tests>`_ with +90% coverage
4. declare your connector in `config.yml <config.yml>`_ in the `sources` section
5. when possible, provide a docker image that runs the backend service, so we can test the connector
6. if you can't provide a docker image, provide the credentials needed to run against a service
7. The backend needs to return 10,001 documents.


.. note::

   Any patch with changes outside `connectors/sources <connectors/sources>`_ or `config.yml <config.yml>`_
   will be rejected. If you need changes in the framework, reach out the Ingestion team.


Testing the connector
:::::::::::::::::::::

To test the connector, we'll run::

   make test

We require the connector to have a unit test and to have a 90% coverage reported by this command

If this first step pass, we'll start your Docker instance or configure your bakcend, then run::

   make ftest service_type

This will configure the connector in Elasticsearch to run a full sync.
The script will verify that the Elasticsearch index receives 10,001 documents

