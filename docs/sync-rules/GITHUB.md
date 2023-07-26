### Setting up the Github connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Advanced rules for indexing document and files based on branch name configured via branch key

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

#### Advanced rules for indexing document based on issue query related to bugs via issue key

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

#### Advanced rules for indexing document based on pr query related to open pr's via pr key 

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

#### Advanced rules for indexing document and files based on queries and branch name

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

**NOTE**: All documents pulled by a given rule are indexed regardless of whether the document has already been indexed by the previous rule. In such cases, document duplication will happen, but the count of indexed documents will be different in the logs. The correct count of documents can be referred from the ElasticSearch index.

#### Advanced rules for overlapping

```json
[
  {
    "filter": {
      "pr": "is:pr is:merged label:auto-backport merged:>=2023-07-20"
    },
    "repository": "repo_name"
  },
  {
    "filter": {
      "pr": "is:pr is:merged label:auto-backport merged:>=2023-07-15"
    },
    "repository": "repo_name"
  }
]
```
