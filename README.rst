==========
Connectors
==========

Provides a CLI to ingest documents into Elasticsearch, following BYOC & BYOEI standards.

To install the CLI, run:

.. code-block:: bash

   pip install elasticsearch-connectors

The `elastic-ingest` CLI will be installed on your system:

.. code-block:: bash

    elastic-ingest --help
    usage: elastic-ingest [-h] [--action {poll,list}] [-c CONFIG_FILE] [--debug]

    optional arguments:
    -h, --help            show this help message and exit
    --action {poll,list}  What elastic-ingest should do
    -c CONFIG_FILE, --config-file CONFIG_FILE
                            Configuration file
    --debug               Run the event loop in debug mode


Architecture
============

The CLI runs the `ConnectorService <connectors/runner.py>` which is an
asynchronous event loop. It calls Elasticsearch on a regular basis to see if
some syncs need to happen.

That information is provided by Kibana and follows the `BYOC` protocol.
That protocol defines a few structures in a couple of dedicated Elasticsearch
indices, that are used by Kibana to drive sync job, and by the connectors
to report on that work.

When a user asks for a sync of a specific source, the service instanciates
a class that it uses to reach out the source and collect data.

A source class can be any Python class, and is declared into the
`configuration <config.yml>` file. For example:

.. code-block:: yaml

  sources:
    mongo: connectors.sources.mongo:MongoDataSource
    s3: connectors.sources.aws:S3DataSource


This notation is called the Fully Qualified Name (FQN) and tells the
framework where the class is located so it can import it and
instanciate it.

Source classes can be located in this project or any other Python
project, as long as it can be imported.

For example, if the project `mycompany-foo` implements the
source `GoogleDriveDataSource` in the package `gdrive`, we should be able to run:

.. code-block:: bash

   pip install mycompany-foo

And then add in the Yaml file:

.. code-block:: yaml

   sources:
     gdrive: gdrive:GoogleDriveDataSource

And that source will be available in Kibana.


Implementing a new source
=========================

XXX
