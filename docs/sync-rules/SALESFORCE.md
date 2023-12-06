### Setting up the Salesforce connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

Advanced Sync Rules help filter data in Salesforce before indexing into Elasticsearch. They take the following parameters:

1. `query` : Salesforce query to filter the documents.
2. `language` : Salesforce query language. Allowed values are **SOQL** and **SOSL**.

### Advanced sync rules examples

### Fetching documents based on the query and language specified.

#### Example
Fetch documents using SOQL query

```json
[
  {
    "query": "SELECT Id, Name FROM Account",
    "language": "SOQL"
  }
]
```

#### Example
Fetch documents using SOSL query

```json
[
  {
    "query": "FIND {Salesforce} IN ALL FIELDS",
    "language": "SOSL" 
  }
]
```

### Fetching standard and custom objects using SOQL and SOSL query

#### Example
Fetch documents for standard objects via SOQL and SOSL query

```json
[
  {
    "query": "SELECT Account_Id, Address, Contact_Number FROM Account",
    "language": "SOQL"
  },
  {
    "query": "FIND {Alex Wilber} IN ALL FIELDS RETURNING Contact(LastModifiedDate, Name, Address)",
    "language": "SOSL"
  }
]
```

#### Example
Fetch documents for custom objects via SOQL and SOSL query

```json
[
  {
    "query": "SELECT Connector_Name, Version FROM Connector__c",
    "language": "SOQL"
  },
  {
    "query": "FIND {Salesforce} IN ALL FIELDS RETURNING Connectors__c(Id, Connector_Name, Connector_Version)",
    "language": "SOSL"
  }
]
```

### Fetching documents with all standard and custom fields / only custom fields / only standard fields

#### Example
Fetch documents with all standard and custom for Account object

```json
[
  {
    "query": "SELECT FIELDS(ALL) FROM Account",
    "language": "SOQL"
  }
]
```

#### Example
Fetch documents with all custom fields for Connector object

```json
[
  {
    "query": "SELECT FIELDS(CUSTOM) FROM Connector__c",
    "language": "SOQL"
  }
]
```

#### Example
Fetch documents with all standard fields for Account object

```json
[
  {
    "query": "SELECT FIELDS(STANDARD) FROM Account",
    "language": "SOQL"
  }
]
```
