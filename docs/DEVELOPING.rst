==========
Connectors
==========

Provides a CLI to ingest documents into Elasticsearch, following BYOC & BYOEI standards.

To install the CLI, run:

.. code-block:: bash

   $ make install

The `elastic-ingest` CLI will be installed on your system:

.. code-block:: bash

    $ bin/elastic-ingest --help
    usage: elastic-ingest [-h] [--action {poll,list}] [-c CONFIG_FILE] [--debug]

    optional arguments:
    -h, --help            show this help message and exit
    --action {poll,list}  What elastic-ingest should do
    -c CONFIG_FILE, --config-file CONFIG_FILE
                            Configuration file
    --debug               Run the event loop in debug mode


Architecture
============

The CLI runs the `ConnectorService <connectors/runner.py>`_ which is an
asynchronous event loop. It calls Elasticsearch on a regular basis to see if
some syncs need to happen.

That information is provided by Kibana and follows the `BYOC <https://github.com/elastic/connectors-ruby/blob/main/docs/CONNECTOR_PROTOCOL.md>`_ protocol.
That protocol defines a few structures in a couple of dedicated Elasticsearch
indices, that are used by Kibana to drive sync jobs, and by the connectors
to report on that work.

When a user asks for a sync of a specific source, the service instanciates
a class that it uses to reach out the source and collect data.

A source class can be any Python class, and is declared into the
`configuration <config.yml>`_ file. For example:

.. code-block:: yaml

  sources:
    mongodb: connectors.sources.mongo:MongoDataSource
    s3: connectors.sources.aws:S3DataSource


The source class is declared with its `Fully Qualified Name (FQN) <https://en.wikipedia.org/wiki/Fully_qualified_name>`_
so the framework knows where the class is located so it can import it and
instanciate it.

Source classes can be located in this project or any other Python
project, as long as it can be imported.

For example, if the project `mycompany-foo` implements the
source `GoogleDriveDataSource` in the package `gdrive`, we should be able to run:

.. code-block:: bash

   $ pip install mycompany-foo

And then add in the Yaml file:

.. code-block:: yaml

   sources:
     gdrive: gdrive:GoogleDriveDataSource

And that source will be available in Kibana.


Sync strategy
=============

In Workplace Search we have the four following syncs:

- **Full sync** (runs every 72 hours by default): This synchronization job synchronizes all of the data from the content source ensuring full data parity.
- **Incremental sync** (runs every 2 hours by default): This synchronization job synchronizes updates to the data at the content source ensuring high data freshness.
- **Deletion sync** (runs every 6 hours by default): This synchronization job synchronizes document deletions from the content source ensuring regular removal of stale data.
- **Permissions sync** (runs every 5 minutes by default, when Document Level Permissions are enabled): This synchronization job synchronizes document permissions from the content sources ensuring secure access to documents on Workplace Search.

In `connectors-py` we are implementing for now just **Full sync**, which ensures
full data parity (including deletion).

This sync strategy is good enough for some sources like Mongo where 100,000 documents
can be fully synced in less than 30 seconds.

We will introduce more sophisticated syncs as we add new sources, in order to achieve
the same level of freshness we have in Workplace Search.

The **Permissions sync** will be included later as well once we have designed
how Document-Level Permission works in the new architecture.

How a sync works
================

Syncing a backend consists of reconciliating an Elasticsearch index with an
external data source. It's a read-only mirror of the data located in the 3rd
party storage.

To sync both sides, the CLI uses these steps:

- asks the source if something has changed, if not, bail out.
- collects the list of documents IDs and timestamps in Elasticsearch
- iterate on documents provided by the data source class
- for each document

  - if there is a timestamp and it matches the one in Elasticsearch, ignores it
  - if not, adds it as an `upsert` operation into a `bulk` call to Elasticsearch

- for each id from Elasticsearch that is not present it the documents sent by the data source class,
  adds it as a `delete` operation into the `bulk` call
- `bulk` calls are emited every 500 operations (this is configurable for slow networks).


To implement a new source, check `CONTRIBUTE.rst <CONTRIBUTE.rst>`_
