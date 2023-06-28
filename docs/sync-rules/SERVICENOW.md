### Setting up the ServiceNow connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Query for indexing document based on incident number starts with INC001 for Incident service

```json
[
  {
    "service": "Incident",
    "query": "numberSTARTSWITHINC001"
  }
]
```

#### Query for indexing document based on user active state as false for User service

```json
[
  {
    "service": "User",
    "query": "active=False"
  }
]
```

#### Query for indexing document based on author as administrator for Knowledge service 

```json
[
  {
    "service": "Knowledge",
    "query": "author.nameSTARTSWITHSystem Administrator"
  }
]
```
