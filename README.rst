==========
Connectors
==========

Provides a CLI to ingest documents into Elasticsearch, following BYOC & BYOEI standards.

To install the CLI, run:

.. code-block:: bash

   $ pip install elasticsearch-connectors

The `elastic-ingest` CLI will be installed on your system:

.. code-block:: bash

    $ elastic-ingest --help
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

That information is provided by Kibana and follows the `BYOC` protocol.
That protocol defines a few structures in a couple of dedicated Elasticsearch
indices, that are used by Kibana to drive sync job, and by the connectors
to report on that work.

When a user asks for a sync of a specific source, the service instanciates
a class that it uses to reach out the source and collect data.

A source class can be any Python class, and is declared into the
`configuration <config.yml>`_ file. For example:

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
- `bulk` calls are emited every 500 operations.



Implementing a new source
=========================

Implementing a new source is done by creating a new class which responsibility
is to send back documents from the targeted source.

Source classes are not required to use any base class as long
as it follows the API signature defined in `BaseDataSource <connectors/source.py>`_:

.. code-block:: python

    class BaseDataSource:
        @classmethod
        def get_default_configuration(cls):
            """Returns a dict with a default configuration
            """
            raise NotImplementedError

        async def changed(self):
            """When called, returns True if something has changed in the backend.

            Otherwise returns False and the next sync is skipped.

            Some backends don't provide that information.
            In that case, this always return True.
            """
            return True

        async def ping(self):
            """When called, pings the backend

            If the backend has an issue, raises an exception
            """
            raise NotImplementedError

        async def get_docs(self):
            """Returns an iterator on all documents present in the backend

            Each document is a mapping with at least an `id` field
            and optionally a `timestamp` field in ISO 8601 UTC
            """
            raise NotImplementedError



Async vs Sync
=============

The CLI uses `asyncio` and makes the assumption that all the code that has been
called should not block the event loop. In order to achieve this asynchronicity
source classes should use async libs for their backend.

When not possible, the class should use `run_in_executor <https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools>`_
and run the blocking code in another thread or process.

Assuming the work is I/O bound, the class should use threads. If there's some
heavy CPU-bound computation (encryption work, etc), processes should be used to
avoid `GIL contention <https://realpython.com/python-gil/>`_



