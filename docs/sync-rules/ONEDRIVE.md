### Setting up the OneDrive connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

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
    "userMailAccounts": ["user1-domain@onmicrosoft.com", "user2-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".py"]
  }
]
```

#### Advanced rule for indexing the files and folders inside path '/drive/root:/hello' excluding .md files

```json
[
  {
    "skipFilesWithExtensions": [".md"],
    "parentPathPattern": "/drive/root:/hello"
  }
]
```

#### Advanced rules for indexing files and folders of user 1, user 3 inside path '/drive/root:/abc' excluding .pdf and .py file

```json
[
  {
    "userMailAccounts": ["user1-domain@onmicrosoft.com", "user3-domain@onmicrosoft.com"],
    "skipFilesWithExtensions": [".pdf", ".py"],
    "parentPathPattern": "/drive/root:/abc"
  }
]
```
