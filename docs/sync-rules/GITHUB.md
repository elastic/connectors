### Setting up the Github connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Schema for indexing document and files based on branch name configured via branch key

```json
[
  {
    "repository": "repo_name",
    "filter": {
      "branch": "sync-rules-feature"
    }
  }
]
```

#### Schema for indexing document based on issue query related to bugs via issue key

```json
[
  {
    "repository": "repo_name",
    "filter": {
      "issue": "is:bug"
    }
  }
]
```

#### Schema for indexing document based on pr query related to open pr's via pr key 

```json
[
  {
    "repository": "repo_name",
    "filter": {
      "pr": "is:open"
    }
  }
]
```

#### Schema for indexing document and files based on queries and branch name

```json
[
  {
    "repository": "repo_name",
    "filter": {
      "issue": "is:bug",
      "pr": "is:open",
      "branch": "sync-rules-feature"
    }
  }
]
```
