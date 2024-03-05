### Setting up the Notion connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Overview

Advanced Sync Rules help filter data in Notion before indexing into Elasticsearch. They take the following parameters:

1. `searches`: Notion's search filter to search by title.
2. `database_query_filters`: Notion's database query filter to fetch a specific database.

### Example advanced sync rules

#### Advanced rule for indexing every page with title containing `Demo Page`

```json
[
  {
    "searches": [
      {
        "filter": {
          "value": "page"
        },
        "query": "Demo Page"
      }
    ]
  }
]
```

#### Advanced rule for indexing every database with title containing `Demo Database`

```json
[
  {
    "searches": [
      {
        "filter": {
          "value": "database"
        },
        "query": "Demo Database"
      }
    ]
  }
]
```

#### Advanced rule for indexing every database with title containing `Demo Database` and every page with title containing `Demo Page`

```json
[
  {
    "searches": [
      {
        "filter": {
          "value": "database"
        },
        "query": "Demo Database"
      },
      {
        "filter": {
          "value": "page"
        },
        "query": "Demo Page"
      }
    ]
  }
]
```

#### Advanced rule for indexing all the pages which are connected with the integration

```json
[
  {
    "searches": [
      {
        "filter": {
          "value": "page"
        },
        "query": ""
      }
    ]
  }
]
```

#### Advanced rule for indexing all the pages and databases connected with the integration

```json
[
    {
      "searches": [
      {
        "query": ""
      }
    ]
    }
]
```

#### Advanced rule for indexing all the rows of the database where the record is "true" for column "Task completed" and its property(datatype) is checkbox

```json
[
  {
    "database_query_filters": [
      {
        "filter": {
            "property": "Task completed",
            "checkbox": {
                "equals": true
            }
        },
        "database_id": "database_id"
      }
    ]
  }
]
```

#### Advanced rule for indexing all the rows of the specific database 

```json
[
  {
    "database_query_filters": [
      {
        "database_id": "database_id"
      }
    ]
  }
]
```

#### Advanced rule for indexing all blocks defined in the "database_query_filters" and "searches"

```json
[
    {
    "searches" : [
    {
    "query": "External tasks",
        "filter": {
            "value": "database"
            },
        },
    {
    "query": "External tasks",
        "filter": {
        "value": "page"
            }
        }
  ],

  "database_query_filters" : [
    {
    "database_id": "notion_database_id1",
    "filter": ​​{
		"property": "Task completed",
	      "checkbox": {
	        "equals": true
	      }
	    }
    }
  ]
}

]
```
**Note** 
For `database_query_filters`:
    - `property` contains a string that matches the database column name.
    - `checkbox` this is not constant depends on the column name property type it can be `date`,`files`,`formula`,`multi_select`,`number`,`people`,`phone_number`,`relation`,`rich_text`,`select`,`status`,`timestamp` or `ID`
    -`equals` also this is not constant depends on the poperty type if property type is `checkbox` then the possible values are `equals` or `does_not_equal` possible values can be (https://developers.notion.com/reference/post-database-query-filter)

