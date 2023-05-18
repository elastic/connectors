# Connectors Developer's Guide

- [General Configuration](#general-configuration)
- [Installation](#installation)
- [Architecture](#architecture)
- [Syncing](#syncing)
  - [Sync Strategy](#sync-strategy)
  - [How a sync works](#how-a-sync-works)
- [Runtime dependecies](#runtime-dependencies)
- [Implementing a new source](#implementing-a-new-source)
  - [Sync rules](#sync-rules)
    - [Basic rules vs advanced rules](#basic-rules-vs-advanced-rules)
    - [How to implement advanced rules](#how-to-implement-advanced-rules)
    - [How to validate advanced rules](#how-to-validate-advanced-rules)
    - [How to provide custom basic rule validation](#how-to-provide-custom-basic-rule-validation)
  - [Testing the connector](#testing-the-connector)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
- [Async vs Sync](#async-vs-sync)

## General Configuration

The details of Elastic instance and other relevant fields such as `service` and `source` needs to be provided in the [config.yml](https://github.com/elastic/connectors-python/blob/8.6/config.yml) file. For more details check out the following [documentation](https://github.com/elastic/connectors-python/blob/8.6/docs/CONFIG.md).

## Installation

To install, run:
```shell
$ make clean install
```

The `elastic-ingest` CLI will be installed on your system:

```shell
$ bin/elastic-ingest --help
usage: elastic-ingest [-h] [--action {poll,list}] [-c CONFIG_FILE] [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL} | --debug] [--filebeat] [--version] [--uvloop]

options:
  -h, --help            show this help message and exit
  --action {poll,list}  What elastic-ingest should do
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        Configuration file
  --log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}
                        Set log level for the service.
  --debug               Run the event loop in debug mode (alias for --log-level DEBUG)
  --filebeat            Output in filebeat format.
  --version             Display the version and exit.
  --uvloop              Use uvloop if possible
```

Users can execute `make run` command to run the elastic-ingest process in `debug` mode. For more details check out the following [documentation](./CONFIG.md)

## Architecture

The CLI runs the [ConnectorService](../connectors/runner.py) which is an asynchronous event loop. It calls Elasticsearch on a regular basis to see if some syncs need to happen.

That information is provided by Kibana and follows the [connector protocol](https://github.com/elastic/connectors-python/blob/main/docs/CONNECTOR_PROTOCOL.md). That protocol defines a few structures in a couple of dedicated Elasticsearch indices, that are used by Kibana to drive sync jobs, and by the connectors to report on that work.

When a user asks for a sync of a specific source, the service instantiates a class that it uses to reach out the source and collect data.

A source class can be any Python class, and is declared into the [configuration](../config.yml) file (See [Configuration](./CONFIG.md) for detailed explanation). For example:

```yaml
sources:
  mongodb: connectors.sources.mongo:MongoDataSource
  s3: connectors.sources.s3:S3DataSource
```

The source class is declared with its [Fully Qualified Name(FQN)](https://en.wikipedia.org/wiki/Fully_qualified_name) so the framework knows where the class is located, so it can import it and instantiate it.

Source classes can be located in this project or any other Python project, as long as it can be imported.

For example, if the project `mycompany-foo` implements the source `GoogleDriveDataSource` in the package `gdrive`, we should be able to run:

```shell
$ pip install mycompany-foo
```

And then add in the Yaml file:

```yaml
sources:
  gdrive: gdrive:GoogleDriveDataSource
```

And that source will be available in Kibana.

## Syncing
### Sync strategy

In Elastic Python connectors we implement a **Full sync**, which ensures full data parity (including deletion).

This sync strategy is good enough for some sources like MongoDB where 100,000 documents can be fully synced in less than 30 seconds.

We will introduce more sophisticated syncs as we add new sources, in order to achieve the same level of freshness we have in Workplace Search.

### How a sync works

Syncing a backend consists of reconciliating an Elasticsearch index with an external data source. It's a read-only mirror of the data located in the 3rd party data source.

To sync both sides, the CLI uses these steps:

- asks the source if something has changed, if not, bail out.
- collects the list of documents IDs and timestamps in Elasticsearch
- iterate on documents provided by the data source class
- for each document
  - if there is a timestamp, and it matches the one in Elasticsearch, ignores it
  - if not, adds it as an `upsert` operation into a `bulk` call to Elasticsearch
- for each id from Elasticsearch that is not present in the documents sent by the data source class, adds it as a `delete` operation into the `bulk` call
- `bulk` calls are emitted every 500 operations (this is configurable for slow networks).

To implement a new source, check [CONTRIBUTE.rst](./CONTRIBUTING.md)


## Implementing a new source

Implementing a new source is done by creating a new class which responsibility is to send back documents from the targeted source.

Source classes are not required to use any base class as long as it follows the API signature defined in [BaseDataSource](../connectors/source.py).

Check out an example in [directory.py](../connectors/sources/directory.py) for a basic example.

Take a look at the [MongoDB connector](../connectors/sources/mongo.py) for more inspiration. It's pretty straightforward and has that nice little extra feature some other connectors can't implement easily: the [Changes](https://www.mongodb.com/docs/manual/changeStreams/) stream API allows it to detect when something has changed in the MongoDB collection. After a first sync, and as long as the connector runs, it will skip any sync if nothing changed.

Each connector will have their own specific behaviors and implementations. When a connector is loaded, it stays in memory, so you can come up with any strategy you want to make it more efficient. You just need to be careful not to blow memory.

### Sync rules

#### Basic rules vs advanced rules

Sync rules are made up of basic and advanced rules.
Basic rules are implemented generically on the framework-level and work out-of-the-box for every new connector.
Advanced rules are specific to each data source.
Learn more about sync rules [in the Enterprise Search documentation](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html).

Example:

For MySQL we've implemented advanced rules to pass custom SQL queries directly to the corresponding MySQL instance.
This offloads a lot of the filtering to the data source, which helps reduce data transfer size.
Also, data sources usually have highly optimized and specific filtering capabilities you may want to expose to the users of your connector.
Take a look at the `get_docs` method in the [MySQL connector](../connectors/sources/mysql.py) to see an advanced rules implementation.

#### How to implement advanced rules

When implementing a new connector follow the API of the [BaseDataSource](../connectors/source.py).
The custom implementation for advanced rules is usually located inside the `get_docs` function:

```python
async def get_docs(self, filtering=None):
    if filtering and filtering.has_advanced_rules():
        advanced_rules = filtering.get_advanced_rules()
        # your custom advanced rules implementation
    else:
        # default fetch all data implementation 
```

For example, here you could pass custom queries to a database.
The structure of the advanced rules depends on your implementation and your concrete use case.
For MySQL the advanced rules structure looks like this:
You specify databases on the top level, which contain tables, which specify a custom query.

Example:

```json
{
  "database_1": {
    "table_1": "SELECT ... FROM ...;",
    "table_2": "SELECT ... FROM ...;"
  },
  "database_2": {
    "table_1": "SELECT ... FROM ...;"
  }
}
```

Note that the framework calls `get_docs` with the parameter `filtering` of type `Filter`, which is located in [byoc.py](../connectors/byoc.py).
The `Filter` class provides convenient methods to extract advanced rules from the filter and to check whether advanced rules are present.

#### How to validate advanced rules

To validate advanced rules the framework takes the list of validators returned by the method `advanced_rules_validators` and calls them in the order they appear in that list.
By default, this list is empty in the [BaseDataSource](../connectors/source.py) as advanced rules are always specific to the connector implementation.
Plug in custom validators by implementing a class containing a `validate` method, which accepts one parameter.
The framework expects the custom validators to return a `SyncRuleValidationResult`, which can be found in [validation.py](../connectors/filtering/validation.py).

```python
class MyValidator(AdvancedRulesValidator):
    
    def validate(self, advanced_rules):
        # custom validation logic
        return SyncRuleValidationResult(...)
```

Note that the framework will call `validate` with the parameter `advanced_rules` of type `dict`.
Now you can return a list of validator instances in `advanced_rules_validators`:

```python
class MyDataSource(BaseDataSource):

    def advanced_rules_validators(self):
        return [MyValidator()]
```

The framework will handle the rest: scheduling validation, calling the custom validators and storing the corresponding results.

#### How to provide custom basic rule validation

We don't recommend fully overriding `basic_rule_validators`, because you'll lose the default validations.

The framework already provides default validations for basic rules.
To extend the default validation, provide custom basic rules validators.
There are two possible ways to validate basic rules:
- **Every rule gets validated in isolation**. Extend the class `BasicRuleValidator` located in [validation.py](../connectors/filtering/validation.py):
    ```python
    class MyBasicRuleValidator(BasicRuleValidator):
        
        @classmethod
        def validate(cls, rule):
            # custom validation logic
            return SyncRuleValidationResult(...)
    ```
- **Validate the whole set of basic rules**. If you want to validate constraints on the set of rules, for example to detect duplicate or conflicting rules. Extend the class `BasicRulesSetValidator` located in [validation.py](../connectors/filtering/validation.py):
    ```python
    class MyBasicRulesSetValidator(BasicRulesSetValidator):
        
        @classmethod
        def validate(cls, set_of_rules):
            # custom validation logic
            return SyncRuleValidationResult(...)
    ```

To preserve the default basic rule validations and extend these with your custom logic, override `basic_rules_validators` like this:
```python
class MyDataSource(BaseDataSource):

    @classmethod
    def basic_rules_validators(self):
        return BaseDataSource.basic_rule_validators() 
                + [MyBasicRuleValidator, MyBasicRulesSetValidator]
```

Again the framework will handle the rest: scheduling validation, calling the custom validators and storing the corresponding results.

### Testing the connector

#### Unit Tests
To test the connectors, run:
```shell
make test
```

We require all connectors to have unit tests and to have a 90% coverage reported by this command

#### Integration Tests
If the above tests pass, start your Docker instance or configure your backend, then run:
```shell
make ftest NAME={service type}
```

This will configure the connector in Elasticsearch to run a full sync. The script will verify that the Elasticsearch index receives documents.


## Async vs Sync

The CLI uses `asyncio` and makes the assumption that all the code that has been called should not block the event loop. This makes syncs extremely fast with no memory overhead. In order to achieve this asynchronicity, source classes should use async libs for their backend.

When not possible, the class should use [run_in_executor](https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools) and run the blocking code in another thread or process.

When you send work in the background, you will have two options:

- if the work is I/O-bound, the class should use threads
- if there's some heavy CPU-bound computation (encryption work, etc), processes should be used to avoid [GIL contention](https://realpython.com/python-gil/)

When building async I/O-bound connectors, make sure that you provide a way to recycle connections and that you can throttle calls to the backends. This is very important to avoid file descriptors exhaustion and hammering the backend service.


## Runtime dependencies

- MacOS or Linux server. The connector has been tested with CentOS 7, MacOS Monterey v12.0.1.
- Python version 3.10 or later.
- To fix SSL certificate verification failed error, users have to run this to connect with Amazon S3:
    ```shell
    $ System/Volumes/Data/Applications/Python\ 3.10/Install\ Certificates.command
    ```
