# elastic-connectors

## Overview

`elastic-connectors` is an async-first Python package that provides connectors to various third-party services. Each connector class in this package exposes an asynchronous method to fetch documents from the third-party service.

## Installation

To install the package, use pip:

```bash
pip install elastic-connectors
```

## Usage

### Importing a Connector
Each connector module can be imported as follows:

```python
from elastic_connectors import SharepointOnlineConnector
```

### Constructor
The constructor for each connector module requires arguments relevant to the third-party integration along with optional parameters:

- `logger` (logging.Logger, optional): Logger instance. Defaults to None.
- `download_content` (bool, optional): Flag to determine if content should be downloaded. Defaults to True.

### Methods

Each connector module exposes the following asynchronous method to fetch the data from a 3rd party source:

```python
async def async_get_docs(self) -> AsyncIterator[Dict]:
    """
    Asynchronously retrieves documents from the third-party service.

    Yields:
        AsyncIterator[Dict]: An asynchronous iterator of dictionaries containing document data.
    """
```

### Example
Below is an example demonstrating how to use a connector module

```python
docs = []

async with SharepointOnlineConnector(
    tenant_id=SPO_TENANT_ID,
    tenant_name=SPO_TENANT_NAME,
    client_id=SPO_CLIENT_ID,
    secret_value=SPO_CLIENT_SECRET
) as connector:
    spo_docs = []
    async for doc in connector.async_get_docs():
        spo_docs.append(doc)
```

### API overview

See [API overview](./docs/README.md) with all available connectors.
