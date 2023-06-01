### Setting up the Confluence connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### One query for indexing data that is in a particular Space with key 'DEV'

```json
[
  {
    "query": "space = DEV"
  }
]
```

#### Two queries for indexing data based on created and lastmodified time

```json
[
  {
    "query": "created >= now('-5w')"
  },
  {
    "query": "lastmodified < startOfYear()"
  }
]
```

#### One query for indexing only given types in a space with space key 'SD'
```json
[
  {
    "query": "type in ('page', 'attachment') AND space.key = 'SD'"
  }
]
```

### Limitations

- Syncing the recently created/updated items in Confluence may be delayed when using Advanced Sync Rules since the search endpoint used with cql query returns stale results in the response. For more details refer the following issue: https://jira.atlassian.com/browse/CONFCLOUD-73997
