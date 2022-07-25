How to contribute connectors
============================


Implementing a new source
:::::::::::::::::::::::::

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

            Each document is a tuple with:
            - a mapping with the data to index
            - a coroutine to download extra data (attachments)

            The mapping should have least an `id` field
            and optionally a `timestamp` field in ISO 8601 UTC

            The coroutine is called if the document needs to be synced
            and has attachements. It need to return a mapping to index.

            It has two arguments: doit and timestamp
            If doit is False, it should return None immediatly.
            If timestamp is provided, it should be used in the mapping.

            Example:

               async def get_file(doit=True, timestamp=None):
                   if not doit:
                       return
                   return {'TEXT': 'DATA', 'timestamp': timestamp,
                           'id': 'doc-id'}
            """
            raise NotImplementedError


Async vs Sync
:::::::::::::

The CLI uses `asyncio` and makes the assumption that all the code that has been
called should not block the event loop. In order to achieve this asynchronicity
source classes should use async libs for their backend.

When not possible, the class should use `run_in_executor <https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools>`_
and run the blocking code in another thread or process.

Assuming the work is I/O-bound, the class should use threads. If there's some
heavy CPU-bound computation (encryption work, etc), processes should be used to
avoid `GIL contention <https://realpython.com/python-gil/>`_


Contribution Checklist
::::::::::::::::::::::


If you want to add a new connector source, you need to:

1. add a module or a directory in `connectors/sources <connectors/sources>`_
2. implement a class that implements all methods described in `connectors.source.BaseDataSource`
3. add a unit test in `connectors/sources/tests <connectors/sources/tests>`_ with +90% coverage
4. declare your connector in `config.yml <config.yml>`_ in the `sources` section
5. declare your dependencies in `requirements.txt <requirements.txt>`_. Make sure you pin these dependencies
6. make sure you use an async lib for your source. If not possible, make sure you don't block the loop
7. when possible, provide a docker image that runs the backend service, so we can test the connector
8. if you can't provide a docker image, provide the credentials needed to run against a service
9. the test backend needs to return 10,001 documents due to 10,000 being a default size limit for Elasticsearch pagination. Having 10,001 documents returned from the test backend will help testing connector more deeply


.. warning::

   Any patch with changes outside `connectors/sources <connectors/sources>`_ or `config.yml <config.yml>`_
   and `requirements.txt <requirements.txt>`_ will be rejected.

   If you need changes in the framework, reach out to the `Ingestion team <https://github.com/orgs/elastic/teams/ingestion-team/members>`_.


Testing the connector
:::::::::::::::::::::

To test the connector, we'll run::

   make test

We require the connector to have a unit test and to have a 90% coverage reported by this command

If this first step pass, we'll start your Docker instance or configure your bakcend, then run::

   make ftest service_type

This will configure the connector in Elasticsearch to run a full sync.
The script will verify that the Elasticsearch index receives 10,001 documents

