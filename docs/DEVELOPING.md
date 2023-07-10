# Connectors Developer's Guide

- [General Configuration](#general-configuration)
- [Installation](#installation)
- [Architecture](#architecture)
- [Features](#features)
- [Syncing](#syncing)
  - [Sync Strategy](#sync-strategy)
  - [How a sync works](#how-a-sync-works)
- [Runtime dependencies](#runtime-dependencies)
- [Implementing a new source](#implementing-a-new-source)
  - [Common patterns](#common-patterns)
    - [Source/Client](#sourceclient)
    - [Async](#async)
  - [Other considerations](#other-considerations)
  - [Sync rules](#sync-rules)
    - [Basic rules vs advanced rules](#basic-rules-vs-advanced-rules)
    - [How to implement advanced rules](#how-to-implement-advanced-rules)
    - [How to validate advanced rules](#how-to-validate-advanced-rules)
    - [How to provide custom basic rule validation](#how-to-provide-custom-basic-rule-validation)
  - [Testing the connector](#testing-the-connector)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
- [Google Drive Connector](#google-drive-connector)


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

For example, if the project `mycompany-foo` implements the source `GoogleDriveDataSource` in the package `gdrive`, run:

```shell
$ pip install mycompany-foo
```

And then add in the Yaml file:

```yaml
sources:
  gdrive: gdrive:GoogleDriveDataSource
```

And that source will be available in Kibana.

## Features

Connector can support features that extend its functionality, allowing for example to have customizable filtering on the edge, permissions or incremental syncs.

The features are defined as class variables of the data source class, and the available features and default values are:
1. `basic_rules_enabled` (Default: `True`) - whether basic sync rules are enabled
2. `advanced_rules_enabled` (Default: `False`) - whether advanced rules are enabled
3. `dls_enabled` (Default: `False`) - whether document-level security is enabled
4. `incremental_sync_enabled` (Default: `False`) - whether incremental sync is enabled

If you want to enable/disable any feature which is different from the default value, you need to provide the class variable in the data source implementation. E.g. to enable incremental sync:
```python
class MyDataSource(BaseDataSource):
    incremental_sync_enabled = True
```

## Syncing
### Sync strategy

There are two types of content sync jobs: full sync and incremental sync.

A **Full sync** ensures full data parity (including deletion), this sync strategy is good enough for some sources like MongoDB where 100,000 documents can be fully synced in less than 30 seconds.

An **Incremental sync** only syncs documents created, updated and deleted since the last successful content sync.

**Note**: Currently, only Sharepoint Online connector supports incremental sync.

### How a full sync works

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
- If incremental sync is available for the connector, `self._sync_cursor` should also be updated in the connector document in the end of the sync, so that when an incremental sync starts, it knows where to start from.

To implement a new source, check [CONTRIBUTE.rst](./CONTRIBUTING.md)

### How an incremental sync works

If a connector supports incremental sync, you should first enable it. Check [Features](#features).

You have to implement `get_docs_incrementally` method of the data source class, which will take `sync_cursor` as a parameter, and yield
1. `document` - a json representation of an item in the remote source.
2. `lazy_download` - a function to download attachment, if available.
3. `operation` - operation to be applied to this document.

The possible values of the `operation` are:
1. `index` - the document will be re-indexed.
2. `update` - the document will be partially updated.
3. `delete` - the document will be deleted.

```python
async def get_docs_incrementally(self, sync_cursor, filtering=None):
    # start syncing from sync_cursor
    async for doc in self.delta_change(sync_cursor, filtering):
        yield doc["document"], doc["lazy_download"], doc["operation"]

    # update sync_cursor
    self._sync_cursor = ...
```

`sync_cursor` is a dictionary object to record where the last sync job is left off, so that increment sync will know where to start from. The structure of the `sync_cursor` is connector dependent. E.g. in Sharepoint Online connector, we want to sync site drives incrementally, we could design the sync cursor to be like:
```json
{
  "site_drives": {
    "{site_drive_id}" : "{delta_link}",
    ...
  }
}
```

If increment sync is enabled for a connector, you need to make sure `self._sync_cursor` is updated at the end of both full and incremental sync. Take a look at [Sharepoint Online connector](../connectors/sources/sharepoint_online.py) implementation as an example.

## Implementing a new source

Implementing a new source is done by creating a new class which responsibility is to send back documents from the targeted source.

Source classes are not required to use any base class as long as it follows the API signature defined in [BaseDataSource](../connectors/source.py).

Check out an example in [directory.py](../connectors/sources/directory.py) for a basic example.

Take a look at the [MongoDB connector](../connectors/sources/mongo.py) for more inspiration. It's pretty straightforward and has that nice little extra feature some other connectors can't implement easily: the [Changes](https://www.mongodb.com/docs/manual/changeStreams/) stream API allows it to detect when something has changed in the MongoDB collection. After a first sync, and as long as the connector runs, it will skip any sync if nothing changed.

Each connector will have their own specific behaviors and implementations. When a connector is loaded, it stays in memory, so you can come up with any strategy you want to make it more efficient. You just need to be careful not to blow memory.

### Common Patterns

#### Source/Client

Many connector implementations will need to interface with some 3rd-party API in order to retrieve data from a remote system.
In most instances, it is helpful to separate concerns inside your connector code into two classes.
One class for the business logic of the connector (this class usually extends `BaseDataSource`), and one class (usually called something like `<3rd-party>Client`) for the API interactions with the 3rd-party.
By keeping a `Client` class separate, it is much easier to read the business logic of your connector, without having to get hung up on extraneous details like  authentication, request headers, error response handling, retries, pagination, etc.

#### Logging

Logging in connector implementation needs to be done via the instance variable `_logger` of `BaseDataSource`, e.g. `self._logger.info(...)`, which will automatically include the context of the sync job, e.g. sync job id, connector id.

If there are other classes, e.g. `<3rd-party>Client`, you need to implement a `_set_internal_logger` method in the source class, to pass the logger to client class. Example of Confluence connector:

```python
def _set_internal_logger(self):
    self.confluence_client.set_logger(self._logger)
```

#### Async

The CLI uses `asyncio` and makes the assumption that all the code that has been called should not block the event loop. This makes syncs extremely fast with no memory overhead. In order to achieve this asynchronicity, source classes should use async libs for their backend.

When not possible, the class should use [run_in_executor](https://docs.python.org/3/library/asyncio-eventloop.html#executing-code-in-thread-or-process-pools) and run the blocking code in another thread or process.

When you send work in the background, you will have two options:

- if the work is I/O-bound, the class should use threads
- if there's some heavy CPU-bound computation (encryption work, etc), processes should be used to avoid [GIL contention](https://realpython.com/python-gil/)

When building async I/O-bound connectors, make sure that you provide a way to recycle connections and that you can throttle calls to the backends. This is very important to avoid file descriptors exhaustion and hammering the backend service.

### Other considerations

When creating a new connector, there are a number of topics to thoughtfully consider before you begin coding.
Below, we'll walk through each with the example of Sharepoint Online.
However, the answers to these topics will differ from connector to connector.
Do not just assume! Always consider each of the below for every new connector you develop.

- [How is the data fetched from the 3rd-party?](#how-is-the-data-fetched-from-the-3rd-party)
  - [what credentials will a client need?](#what-credentials-will-a-client-need)
    - [if these credentials expire, how will they be refreshed?](#if-these-credentials-expire-how-will-they-be-refreshed)
  - [does the API have rate limits/throttling/quotas that the connector will need to respect?](#does-the-api-have-rate-limitsthrottlingquotas-that-the-connector-will-need-to-respect)
  - [What is the API's default page size?](#what-is-the-apis-default-page-size)
  - [When should failure responses be retried?](#when-should-failure-responses-be-retried)
  - [What is the permission model used by the 3rd-party?](#what-is-the-permission-model-used-by-the-3rd-party)
- [What is the expected performance for the connector?](#what-is-the-expected-performance-for-the-connector)
  - [how much memory should it require?](#how-much-memory-should-it-require)
  - [will it require local file (disk) caching?](#will-it-require-local-file--disk--caching)

#### How is the data fetched from the 3rd-party?
Perhaps there is one main "documents API" that returns all the data.
This type of API is common for document stores that are relatively flat, or for APIs that are focused on time-series data.
Other APIs are more graph-like, where a top-level list of entities must be retrieved, and then the children of each of those must be retrieved.
This type of API can sometimes be treated as a kind of recursive graph traversal.
Other times, each type of entity needs to be explicitly retrieved and handled with separate logic.

In the case of Sharepoint Online, the data inside is a tree structure:

```
Site Collection
    └── Site
         ├─── Drive
         │    └── Drive Item - - - Drive Item (recursive, optional)
         ├─── List
         │    └── List Item - - - Drive Item (optional)
         └── Site Page
```

Unfortunately, some of the features available in the Sharepoint REST API are not yet available in Graph API - namely access to Page content.
Going through this exercise allows you to identify the need to use two different APIs in order to fetch data.

##### What credentials will a client need?

3rd-party APIs nearly always require authentication.
This authentication can differ between the username/password used by a human interacting with the software through a user experience built by the 3rd-party and the programmatic credentials necessary to use their API.
Understanding if the connector will use basic auth, API keys, OAuth, or something else, is critical to deciding what types of configuration options need to be exposed in Kibana.

Sharepoint Online uses an OAuth2 Client Credentials Flow.
This requires initial values for:
- `client_id`
- `client_secret`
- `tenant_id`
- `scopes`

so that these can be exchanged for an OAuth2 Access Token with logic like:

```python
    authority = f"https://login.microsoftonline.com/{tenant_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority)

    result = app.acquire_token_for_client(scopes=scopes)

    access_token = result["access_token"]
```

`scopes` is a value that will not change from one connection to another, so it can be hardcoded in the connector.
All the others, however, will need to be exposed to the customer as configurations, and will need to be treated sensitively.
Do not log the values of credentials!

###### If these credentials expire, how will they be refreshed?

It is modern best-practice to assume that any credential can and will be revoked.
Even passwords in basic-auth _should_ be rotated on occasion.
Connectors should be designed to accommodate this.
Find out what types of errors the 3rd-party will raise if invalid or expired credentials are submitted, and be sure to recognize these types of errors for what they are.
Handling these errors so that they display in Kibana like, `"Your configured credentials have expired, please re-configure"` is significantly preferable over, `"Sync failed, see logs for details: Error: 401 Unauthorized"`.
Even better than that is to not issue an error at all, but to instead automatically refresh the credentails and retry.

Sharepoint Online, using the OAuth2 Client Credentials Flow, expects the `access_token` to frequently expire.
When it does, the relevant exception can be caught, analyzed to ensure that the root cause is an expired access_token, the token can be refreshed using the configured credentials, and then any requests that have failed in the mean time can be retried.

If the `client_secret` is revoked, however, automatic recovery is not possible.
Instead, you should catch the relevant exception, ensure that refreshing the `access_token` is not possible, and raise an error with an actionable error message that an operator will see in Kibana and/or the logs.


##### Does the API have rate limits/throttling/quotas that the connector will need to respect?

Often times, 3rd-party APIs (especially those of SaaS products) have API "quotas" or "rate limits" which prevent a given client from issuing too many requests too fast.
This protects the service's general health and responsiveness, and can be a security practice to protect against bad actors.

When writing your connector, this is important to keep in mind, that non-200 responses _may not mean_ that anything is wrong, but merely that you're sending too many requests, too fast.

There are two things you should do to deal with this:
1. Be as efficient in your traffic as possible.
  - Use bulk requests over individual requests
  - Increase your page sizes
  - Cache records in-memory rather than looking them up multiple times
2. Respect the rate limit errors
  - handle (don't crash on) 429 errors
  - read the `Retry-After` header
  - know if the API uses mechanisms other than 429 codes and `Retry-After`

For Sharepoint Online, there are two important limits for the Sharepoint API:

- Per minute app can consume between 1,200 to 6,000 Resource Units.
- Per day app can consume between 1,200,000 to 6,000,000 Resource Units.

These quotas are shared between Graph API and Sharepoint REST API - the latter consumes more resource units.

If the quotas are exceeded, some endpoints return 503 responses which contain the `Retry-After` header, and are used instead of 429 codes.

Putting all this together, you now know that you must specially handle 503 and 429 responses, looking for `Retry-After` headers,
and design your connector knowing that you must make the absolute most out of each request, since you will nearly always run up on the per-minute and per-day limits.

##### What is the API's default page size?

Related to the above section, many developers are surprised at how small default page sizes may be.
If your goal is to reduce the number of requests you make to a 3rd-party, page sizes of 10 or 20 rarely make sense.

However, page sizes of 1,000,000,000,000 likely aren't a good idea either, no matter how small a given record is.
It's important to consider how large of a response payload you can reasonably handle at once, and craft your page size accordingly.

Sharepoint Online has a default page size of 100.
For smaller-payload resources like Users and Groups, this can probably be safely increased by an order of magnitude or more.

##### When should failure responses be retried?

As discussed above, many APIs utilize rate limits and will issue 429 responses for requests that should be retried later.
However, sometimes there are other cases that warrant retries.

Some services experience frequent-but-transient timeout issues.
Some services experiencing a large volume of write operations may sometimes respond with 409 "conflict" codes.
Sometimes downloading large files encounter packet loss, and while the response is a 200 OK, the checksum of the downloaded file does not match.

All of these are operations that may make sense to retry a time or two.

A note on retries - do not retry infinitely, and use backoff.
A service may block your connector or specific features if you retry too aggressively.

##### What is the permission model used by the 3rd-party?

Whether or not you plan to support Document Level Security with your connector, it is important to understand how the 3rd-party system protects data and restricts access.
This allows you to ensure that synced documents contain the relevant metadata to indicate ownership/permissions so that any consumer of your connector can choose to implement their own DLS if they so desire.

For Sharepoint Online, for example, this involves syncing the groups and users that own a document or to whom a document has been shared to.
Sometimes these ownership models can be complex.
For Sharepoint Online, you must consider:
- the individual creator of the document
- the "owners" of any site where the document lives
- the "members" of any site where the document lives
- the "visitors" of any site where the document lives
- Any Office365 groups that this document has been shared with
- Any Sharepoint groups that this document has been shared with
- Any Azure AD groups that this document has been shared with
- Any individuals (email addresses) that this document has been shared with

How this information is serialized onto a document is less important than ensuring that it _is_ included.
One common pattern is to put all permissions information in an `_allow_permissions` field with an array of text values.
But it would be equally valid to split each of the above bullets into their own fields.
Or to have two fields, one for "individuals" and one for "groups".

#### What is the expected performance for the connector?

There are no hard-and-fast rules here, unless you're building a connector with a specific customer in mind, who has specific requirements.
However, it is good to ensure that any connector you build has comparable performance to that of other connectors.
To measure this, you can simply utilize the `make ftest` (functional tests) and look at the generated performance dashboards to compare apples-to-apples.
Pay particular attention to memory, CPU, and file handle metrics.

See [Integration tests](#integration-tests) for more details.

##### How much memory should it require?

In order to understand if your connector is consuming more memory than it really should, you need to understand how big various objects are when they are retrieved from a 3rd-party.

In Sharepoint Online, the object sizes could be thought of like:

- Site Collection - small objects less than a KB
- Site - small objects less than a KB
- Drive - small objects less than a KB
- Drive Item - small objects less than a KB, but require binary download + subextraction and binary content can be as large as file systems allow
- List - small objects less than a KB
- List Item - small objects less than a KB
- Site Page - various size HTML objects that can get large

Therefore, if you have binary content extraction disabled and are seeing this connector take up GB of memory, there's likely a bug.
Conversely, if you are processing and downloading many binary files, it is perfectly reasonable to see memory spike in proportion to the size of said files.

It's also important to note that connectors will store all IDs in memory while performing a sync.
This allows the connector to then remove any documents from the Elasticsearch index which were not part of the batch just ingested.
On large scale syncs, these IDs can consume a significant amount of RAM, especially if they are large.
Be careful to design your document IDs to not be longer than necessary.

##### Will it require local file (disk) caching?

Some connectors require writing to disk in order to utilize OS-level utils (base64, for example), or to buffer large structures to avoid holding them all in memory.
Understanding these requirements can help you to anticipate how many file handles a connector might reasonably need to maintain.

Sharepoint Online, for example, needs to download Site Page and Drive Item objects.
This results in a relatively high number of overall file handles being utilized during the sync, but should not lead to an ever-increasing number of currently active file handles.
Seeing any of none, ever-decreasing, or ever-increasing file handles while monitoring a sync would be indicative of a bug in this case.

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

To implement advanced rules, you should first enable them. Check [Features](#features).

The custom implementation for advanced rules is usually located inside the `get_docs` (and `get_docs_incrementally` if incremental sync is enabled) function:

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

Overriding `basic_rule_validators` is not recommended, because you'll lose the default validations.

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

All connectors are required to have unit tests and to have a 90% coverage reported by this command

#### Integration Tests
If the above tests pass, start your Docker instance or configure your backend, then run:
```shell
make ftest NAME={service type}
```

This will configure the connector in Elasticsearch to run a full sync. The script will verify that the Elasticsearch index receives documents.

## Runtime dependencies

- MacOS or Linux server. The connector has been tested with CentOS 7, MacOS Monterey v12.0.1.
- Python version 3.10 or later.
- To fix SSL certificate verification failed error, users have to run this to connect with Amazon S3:
    ```shell
    $ System/Volumes/Data/Applications/Python\ 3.10/Install\ Certificates.command
    ```


## Google Drive Connector

The [Elastic Google Drive connector](../connectors/sources/google_drive.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

### Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

### Configuring the Google Drive Connector:

You must configure the Google Drive connector before syncing the data. Fot this you need to create a [service account](https://cloud.google.com/iam/docs/service-account-overview) with appropriate access to Google Drive API.

To get started, log into [Google Cloud Platform](cloud.google.com) and go to the `Console`.

1. **Create a Google Cloud Project.** Give your project a name, change the project ID and click the Create button.

2. **Enable Google APIs.** Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable the Drive API.

3. **Create a Service Account.** In the `APIs & Services` section, click on `Credentials` and click on `Create credentials` to create a service account. Give your service account a name and a service account ID. This is like an email address and will be used to identify your service account in the future. Click `Done` to finish creating the service account.

4. **Create a Key File**. In the Cloud Console, go to IAM and Admin > Service accounts page. Click the email address of the service account that you want to create a key for. Click the `Keys` tab. Click the `Add key` drop-down menu, then select `Create new key`. Select JSON as the Key type and then click Create. This will download a JSON file that will contain your private key.

5. **Share Google Drive Folders.** Go to your Google Drive. Right-click the folder or shared drive, choose Share and add the email address of the service account you created in step 3. as a viewer to this folder. Note: When you grant a service account access to a specific folder or shared drive in Google Drive, it's important to note that the permissions extend to all the children within that folder or drive. This means that any folders or files contained within the granted folder or drive inherit the same access privileges as the parent.

### Configuration

The following configuration fields need to be provided for setting up the connector:
##### `Google Drive service account JSON`
For Google Drive API, a service account JSON contains credentials and configuration information for your service account. This is the content of a JSON file obtained from step 4. of **Configuring the Google Drive Connector**.

The service account JSON file includes the following information:

- `Client ID`: A unique identifier for the service account.
- `Client Email`: The email address associated with the service account.
- `Private Key`: A private key used to authenticate requests made by the service account.
- `Private Key ID`: An identifier for the private key.
- `Token URI`: The URI where the service account can obtain an access token.
- `Project ID`: The ID of the Google Cloud project associated with the service account.
- `Auth URI`, Token URI, Auth Provider X509 Cert URL: URLs used for authentication and authorization purposes.
- `Client X509 Cert URL`: The URL of the public key certificate associated with the service account.

### Sync rules
- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.
### E2E Tests
The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.
To perform E2E testing for the Google Drive connector, run the following command:
```shell
$ make ftest NAME=google_drive
```

ℹ️ Users can generate the [perf8](https://github.com/elastic/perf8) report using an argument i.e. `PERF8=True`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a SharePoint source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock SharePoint source using the docker image.

ℹ️ The connector uses the Elastic [ingest attachment processor](https://www.elastic.co/guide/en/enterprise-search/current/ingest-pipelines.html) plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.
