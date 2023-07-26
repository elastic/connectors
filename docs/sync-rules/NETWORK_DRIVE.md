### Setting up the Network Drive connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Overview

The advanced sync rules would contain a set of glob patterns provided by the user, using which the connector brings data from the Network Drive.

1. Each rule contains a glob pattern. This pattern is then matched against all the available folder paths inside the configured drive path.
2. The pattern must begin with the `drive_path` field configured in the connector.
2. If the pattern matches any available folder path(s), the contents directly inside that folder(s) will be fetched.


### Example advanced sync rules

#### Advanced rules for indexing the files and folders recursively inside the given folder 'mock' and 'alpha'

```json
[
  {
    "pattern": "Folder-shared/a/mock/**",
  },
  {
    "pattern": "Folder-shared/b/alpha/**",
  }
]
```

#### Advanced rules for indexing the files and folders directly inside given folder 'test'

```json
[
  {
    "pattern": "Folder-shared/a/b/test",
  }
]
```

#### Advanced rules for indexing the files and folders directly inside set of folders 'test1', 'test3', 'test5' 

```json
[
  {
    "pattern": "Folder-shared/org/*/all-tests/test[135]"
  }
]
```

#### Advanced rules for indexing files and folders except those inside 'test7'

```json
[
  {
    "pattern": "Folder-shared/**/all-tests/test[!7]",
  }
]
```
