Connectors
==========

Ingest documents into Elasticsearch, following BYOC & BYOEI standards.


What the project contains:

- `byoc.py` -- implements the BYOC protocol
- `byoei.py` -- implements the BYOEI protocol
- `runner.py` -- the async loop that polls Elasticsearch for work
- `sources` -- data sources (Mongo, AWS S3, etc.)
- `registry.py` -- data sources plugins registry
