### Setting up the OneDrive connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Overview

The advanced sync rules consists of a set of rules where each rule can have one or more parameters out of these:

1. owners: list of user emails of the owners for which the rule would execute.
2. skipFilesWithExtensions: list of file extensions to be skipped for the sync.
3. parentPathPattern: glob pattern specifying the path where the files to be synced are present. 

### Example advanced sync rules

#### Advanced rule for indexing the files and folders excluding .xlsx and .docx files

```json
[
  {
    "skipFilesWithExtensions": [".xlsx" , ".docx"]
  }
]
```

#### Advanced rule for indexing the files and folders for user 1 and user 2, excluding .py files

```json
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".py"]
  }
]
```

#### Advanced rule for indexing the files and folders directly inside root folder path excluding .md files

```json
[
  {
    "skipFilesWithExtensions": [".md"],
    "parentPathPattern": "/drive/root:"
  }
]
```

#### Advanced rules for indexing the files and folders of user 1, user 3 directly inside folder 'abc' excluding .pdf and .py file

```json
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user3-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".pdf", ".py"],
    "parentPathPattern": "/drive/root:/hello/**/abc"
  }
]
```

### Two Advanced rules for indexing the files and folders of user 1 and user 2, and only .py files of other users

```json
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"]
  },
  {
    "skipFilesWithExtensions": [".py"]
  }
]
```

### Two Advanced rules for indexing the .md files user 1 and user 2 and files/folders recursively inside folder abc 

```json
[
  {
    "owners": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".md"]
  },
  {
    "parentPathPattern": "/drive/root:/abc/**"
  }
]
```
