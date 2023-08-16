### Setting up the Github connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example advanced sync rules

See [search operators you can use with Gmail](https://support.google.com/mail/answer/7190) for all supported operators.

#### Emails from the year 2022 or later

```json
[
  {
    "messages": [
      "after:2021/12/31"
    ]
  }
]
```

#### Emails from user "amy"

```json
[
  {
    "messages": [
      "from:amy"
    ]
  }
]
```

#### Emails before 2021/10/10 or from user amy

```json
[
  {
    "messages": [
      "before:2021/10/10",
      "from:amy"
    ]
  }
]
```

Alternative using the `OR` operator (will only send one request in comparison to the query above, which sends two):
```json
[
  {
    "messages": [
      "before:2021/10/10 OR from:amy"
    ]
  }
]
```

