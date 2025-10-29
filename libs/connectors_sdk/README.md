# Connectors SDK

The Connectors SDK is a framework for writing data connectors. This library is a dependency of the Connectors service found under `app/connectors_service`.

Furthermore, you can use this SDK as a standalone framework to author simple data source connectors without having to ingest data directly into Elasticsearch.

## What's here?
- A `pyproject.toml` file
- The connectors framework code

## Simple code example
```python
import asyncio
from connectors_sdk.source import (
        BaseDataSource,
        DataSourceConfiguration
)

class CustomDataSource(BaseDataSource):
    def __init__(self, configuration):
        super().__init__(configuration=configuration)

    @classmethod
    def get_default_configuration(cls):
        return {
            "max_doc_count": {
                "label": "Maximum nomber of documents",
                "order": "1",
                "tooltip": "Maximum number of documents to return",
                "type":"int",
                "value": 1
            }
        }

    async def get_docs(self):
        # get your data
        data = {
                "document_0451":"A shock to the system.",
                "document_0452":"A Foundation for knowledge.",
                "document_0453":"We CAN count to three.",
                "document_0454":"Gather artifacts from anomalies.",
                "document_0455":"Security is not optional.",
                "document_0456":"The Invicible."
            }

        docs_returned = 0
        for key, value in data.items():
            if docs_returned < self.configuration["max_doc_count"]:
                yield value
                docs_returned += 1
                continue
            break

# Define a main function and run it async
async def main():
    base_config = {"max_doc_count": 3}
    data_source_config = DataSourceConfiguration(base_config)
    # Create the data source object
    data_source = CustomDataSource(data_source_config)
    async for docs in data_source.get_docs():
        print (docs)

asyncio.run(main())
```
