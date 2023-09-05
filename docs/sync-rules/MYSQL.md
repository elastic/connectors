### Setting up the MySQL connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

### MySQL Docker setup

#### Run MySQL container
```shell
docker run --name mysql_container -p 3306:3306 -e MYSQL_ROOT_PASSWORD=changeme -e MYSQL_USER=elastic -e MYSQL_PASSWORD=changeme -d mysql:latest
```

#### Grant privileges to user

```shell
docker exec -it mysql_container mysql -u root -p
```

```mysql
GRANT ALL PRIVILEGES ON sample_db.* TO 'elastic'@'%';
FLUSH PRIVILEGES;
```

### Example data
```mysql
CREATE DATABASE sample_db;
USE sample_db;

CREATE TABLE person (
    person_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    age INT
);

CREATE TABLE address (
    address_id INT AUTO_INCREMENT PRIMARY KEY,
    address VARCHAR(255)
);

INSERT INTO person (name, age) VALUES ('Alice', 30);
INSERT INTO person (name, age) VALUES ('Bob', 25);
INSERT INTO person (name, age) VALUES ('Carol', 35);

INSERT INTO address (address) VALUES ('123 Elm St');
INSERT INTO address (address) VALUES ('456 Oak St');
INSERT INTO address (address) VALUES ('789 Pine St');
```

### Example advanced sync rules

#### Two LIMIT queries

```json
[
  {
    "tables": [
      "person"
    ],
    "query": "SELECT * FROM sample_db.person LIMIT 1;"
  },
  {
    "tables": [
      "address"
    ],
    "query": "SELECT * FROM sample_db.address LIMIT 1;"
  }
]
```

#### One WHERE query

```json
[
  {
    "tables": ["person"],
    "query": "SELECT * FROM sample_db.person WHERE sample_db.person.age > 25;"
  }
]
```

#### One JOIN query
```json
[
  {
    "tables": ["person", "tables"],
    "query": "SELECT * FROM sample_db.person INNER JOIN sample_db.address ON sample_db.person.person_id = sample_db.address.address_id;"
  }
]
```