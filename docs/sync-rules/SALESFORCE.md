### Setting up the Dropbox connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Two rules for indexing content based on the query language specified i.e. SOQL and SOSL.

```json
[
  {
    "query": "SELECT Id, Name FROM Account",
    "language": "SOQL"
  },
  {
    "query": "FIND {Salesforce} IN ALL FIELDS",
    "language": "SOSL" 
  }
]
```

#### Two rules for indexing data for standard and custom object using SOQL query

```json
[
  {
    "query": "SELECT Account_Id, Address, Contact_Number FROM Account",
    "language": "SOQL"
  },
  {
    "query": "SELECT Connector_Name, Version FROM Connector__c",
    "language": "SOQL"
  },
  {
    "query": "SELECT Connector_Name, Version FROM Knowledge__kav",
    "language": "SOQL"
  }
]
```

#### Two rules for indexing data for standard and custom object using SOSL query

```json
[
  {
    "query": "FIND {Alex} IN ALL FIELDS RETURNING Contact",
    "language": "SOSL" 
  },
  {
    "query": "FIND {Salesforce} IN ALL FIELDS RETURNING Connector__c(Id, Connector_Name, Version)",
    "language": "SOSL" 
  }
]
```

#### Rules for indexing data for standard and custom object with all fields / all custom fields / all standard fields

```json
[
  {
    "query": "SELECT FIELDS(ALL) FROM Case",
    "language": "SOQL"
  },
  {
    "query": "SELECT FIELDS(CUSTOM) FROM Connector__c",
    "language": "SOQL"
  },
  {
    "query": "SELECT FIELDS(STANDARD) FROM Account",
    "language": "SOQL"
  }
]
```
