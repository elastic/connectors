### Setting up the Jira connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Two queries for indexing content based on status of Jira issues

```json
[
  {
    "query": "project = Collaboration AND status = 'In Progress'"
  },
  {
    "query": "status IN ('To Do', 'In Progress', 'Closed')"
  }
]
```

#### One query for indexing data based on priority of issues for given projects ProjA, ProjB, ProjC

```json
[
  {
    "query": "priority in (Blocker, Critical) AND project in (ProjA, ProjB, ProjC)"
  }
]
```

#### One query for indexing data based on assignee and created time
```json
[
  {
    "query": "assignee is EMPTY and created < -1d"
  }
]
```
