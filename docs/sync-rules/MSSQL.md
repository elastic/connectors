### Setting up the Microsoft SQL connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### Example data

#### `employee` table

| emp_id | name  | age |
|--------|-------|-----|
| 3      | John  | 28  |
| 10     | Jane  | 35  |
| 14     | Alex  | 22  |

#### `customer` table

| c_id | name | age |
|------|------|-----|
| 2    | Elm  | 24  |
| 6    | Pine | 30  |
| 9    | Oak  | 34  |

### Example advanced sync rules

#### Two queries

```json
[
  {
    "tables": [
      "employee"
    ],
    "query": "SELECT * FROM employee"
  },
  {
    "tables": [
      "customer"
    ],
    "query": "SELECT * FROM customer"
  }
]
```

#### One WHERE query

```json
[
  {
    "tables": ["employee"],
    "query": "SELECT * FROM employee WHERE emp_id > 5"
  }
]
```

#### One JOIN query

```json
[
  {
    "tables": ["employee", "customer"],
    "query": "SELECT * FROM employee INNER JOIN customer ON employee.emp_id = customer.c_id"
  }
]
```
