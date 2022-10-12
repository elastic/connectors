# Elastic Enterprise Search Python connectors

![logo](logo-enterprise-search.png)

The home of Elastic Enterprise Connector Clients written in Python. This
repository contains the framework for customizing Elastic Enterprise Search
native connectors, or writing your own connectors for advanced use cases.

**The connector will be operated by an administrative user from within Kibana.**

> Note: The connector framework is a tech preview feature. Tech preview
> features are subject to change and are not covered by the support SLA of
> general release (GA) features. Elastic plans to promote this feature to GA in
> a future release.


## Installation

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

## Other guides

- [Code of Conduct](https://www.elastic.co/community/codeofconduct)
- [Getting Support](docs/SUPPORT.md)
- [Releasing](docs/RELEASING.md)
- [Developer guide](docs/DEVELOPING.md)
- [Security Policy](docs/SECURITY.md)
- [Elastic-internal guide](docs/INTERNAL.md)
- [Connector Protocol](https://github.com/elastic/connectors-ruby/blob/main/docs/CONNECTOR_PROTOCOL.md)

To implement a new source, check `CONTRIBUTE.rst <CONTRIBUTE.rst>`_
