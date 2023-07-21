### Setting up the Jira connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Two rules for indexing content based on the query only

```json
[
  {
    "query": "confidential"
  },
  {
    "query": "dropbox"
  }
]
```

#### One query for indexing data based on the file_extensions

```json
[
  {
    "query": "dropbox",
    "options": {
      "file_extensions": [
        "txt",
        "pdf"
      ]
    }
  }
]
```

#### One query for indexing data based on file_categories
```json
[
  {
    "query": "test",
    "options": {
      "file_categories": [
        {
          ".tag": "paper"
        },
        {
          ".tag": "png"
        }
      ]
    }
  }
]
```
**Note** 

- `query` contains a string that matches words in the filename.
- In case, both `file_extensions` and `file_categories` are provided, priority is given to `file_categories`.

### Limitations

- Content extraction is not supported for paper files with advanced sync rules enabled.
