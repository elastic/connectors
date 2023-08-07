### Setting up the Dropbox connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

#### Two rules for indexing content based on queries only

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

#### Single query for indexing data based on file extensions

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

#### Single query for indexing data based on file categories
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
- If both `file_extensions` and `file_categories` are provided, priority is given to `file_categories`.

### Limitations

- Content extraction is not supported for Dropbox Paper files when advanced sync rules are enabled.
