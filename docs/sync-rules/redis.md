### Setting up the Redis server/cloud connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Overview

Advanced Sync Rules help manage data in Redis server/cloud. They take the following parameters:

1. `database`: Database indice of Redis server/cloud. Type of value should be integer.
2. `key_pattern`: Pattern of the key for searching in Redis server/cloud
3. `_type`: Type of key(i.e. HASH, LIST, SET, STREAM, STRING, ZSET) in Redis server/cloud.

*NOTE*:

- `key_pattern` or `_type` is not required but any one of them is required to pass.

### Advanced sync rules examples

#### Fetch all keys starting with alpha.

```json
[
  {
    "database": 0,
    "key_pattern": "alpha*"
  }
]


```

#### Fetch database records with exact match by specifying the full key name.

```json
[
  {
    "database": 0,
    "key_pattern": "alpha"
  }
]


```

#### Fetch database records by specifying a character range inside square brackets to match any single character within that range.

```json
[
  {
    "database": 0,
    "key_pattern": "test[123]"
  }
]

```

#### Fetch database records by specifying a character range inside square brackets to exclude characters from the match.

```json
[
  {
    "database": 0,
    "key_pattern": "test[^123]"
  }
]
```

#### Fetch all database records.

```json
[
  {
    "database": 0,
    "key_pattern": "*"
  }
]

```

#### Fetch all database records having redis type SET.

```json
[
  {
    "database": 0,
    "key_pattern": "*",
    "_type": "SET"
  }
]

```

#### Fetch records having redis type SET.

```json
[
  {
    "database": 0,
    "_type": "SET"
  }
]

```