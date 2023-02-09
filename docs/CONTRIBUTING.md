# How to contribute connectors

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

## Async vs Sync

The CLI uses `asyncio` and makes the assumption that all the code that has been called should not block the event loop. This makes syncs extremely fast with no memory overhead. In order to achieve this asynchronicity, source classes should use async libs for their backend.

When not possible, the class should use [run_in_executor](https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools) and run the blocking code in another thread or process.

When you send work in the background, you will have two options:

- if the work is I/O-bound, the class should use threads
- if there's some heavy CPU-bound computation (encryption work, etc), processes should be used to avoid [GIL contention](https://realpython.com/python-gil/)

When building async I/O-bound connectors, make sure that you provide a way to recycle connections and that you can throttle calls to the backends. This is very important to avoid file descriptors exhaustion and hammering the backend service.


## Contribution Checklist

### Initial contribution

If you want to add a new connector source, following requirements are mandatory for the initial patch:

1. add a module or a directory in [connectors/sources](../connectors/sources)
2. implement a class that implements **all methods** described in `connectors.source.BaseDataSource`
3. add a unit test in [connectors/sources/tests](../connectors/sources/tests) with **+90% coverage**
4. **declare your connector** in [config.yml](../config.yml) in the `sources` section
5. **declare your dependencies** in [requirements.txt](../requirements.txt). Make sure you pin these dependencies
6. make sure you use an **async lib** for your source. If not possible, make sure you don't block the loop
7. when possible, provide a **docker image** that runs the backend service, so we can test the connector. If you can't provide a docker image, provide the credentials needed to run against an online service.
8. the **test backend** needs to return more than **10k documents** due to 10k being a default size limit for Elasticsearch pagination. Having more than 10k documents returned from the test backend will help testing connector more deeply

### Enhancements

Enhancements that can be done after initial contribution:

1. the backend meets the performance requirements if we provide some (memory usage, how fast it syncs 10k docs, etc.)
2. update README for the connector client
3. small functional improvements for connector clients


### Other

To make sure we're building great connectors, we will be pretty strict on this checklist, and we will not allow connectors to change the framework code itself.

Any patch with changes outside [connectors/sources](../connectors/sources) or [config.yml](../config.yml) and [requirements.txt](../requirements.txt) will be rejected.

If you need changes in the framework, or you are not sure about how to do something, reach out to the [Ingestion team](https://github.com/orgs/elastic/teams/ingestion-team/members)

For 6, you can look at [Developing with asyncio](https://docs.python.org/3/library/asyncio-dev.html). Asynchronous programming in Python is very concise and produces nice looking code once you understand how it works, but it requires a bit of practice.


## Testing the connector

To test the connector, we'll run:
```shell
make test
```

We require the connector to have a unit test and to have a 90% coverage reported by this command

If this first step pass, we'll start your Docker instance or configure your backend, then run:
```shell
make ftest NAME={service type}
```

This will configure the connector in Elasticsearch to run a full sync. The script will verify that the Elasticsearch index receives documents.
