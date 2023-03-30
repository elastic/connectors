# Connectors Developer's Guide

## Network Drive Connector

The [Elastic Network Drive connector](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/network_drive.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.


### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Network Drive details](#gather-network-drive-details)
2. [Configure Network Drive connector](#configure-network-drive-connector)

#### Gather Network Drive details

Collect the information that is required to connect to your network drive:

- The network drive path the connector will crawl to fetch files. This is the name of the folder shared via SMB.
- The username the connector will use to log in to network drive.
- The password the connector will use to log in to network drive.
- The server IP address where the network drive is hosted.
- The port where the network drive service is hosted.

#### Configure Network Drive connector

The following configuration fields need to be provided for setting up the connector:

##### `username`

The username of the account for network drive. Default value is `admin`.

ℹ️ The user must have atleast **read** permissions for the folder path provided.

##### `password`

The password of the account to be used for crawling the network drive.

##### `server_ip`

The server ip where network drive is hosted. Default value is `127.0.0.1`. Examples:

- `192.158.1.38`
- `127.0.0.1`

##### `server_port`

The server port where network drive service is available. Default value is `445`. Examples:

- `9454`
- `8429`

##### `drive_path`

The network drive path the connector will crawl to fetch files. Examples:

- `Users/perftest`
- `admin/bin`

ℹ️ The drive path should have forward slashes as path separators.

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/network_drive.py#L19).

### Connector Limitations

- Files bigger than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

### E2E Tests

The framework provides a way to test ingestion through a connector against a real data source. This is called a functional test. To execute a functional test for the Network Drive connector, run the following command:
```shell
$ make ftest NAME=network_drive
```

ℹ️ Users do not need to have a running Elasticsearch instance or a Network Drive source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Network Drive source using the docker image.

ℹ️ The e2e test does not currently support testing ingestion against an Elastic Cloud instance. The test uses a dockerized Elasticsearch for the ingestion as configured [here](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/tests/fixtures/network_drive/docker-compose.yml).

ℹ️ The e2e test uses default values defined in [configure Network Drive connector](#configure-network-drive-connector)


## Amazon S3 Connector

The [Elastic Amazon S3 connector](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/s3.py) is used to sync files and file content for supported file types from [Amazon S3](https://s3.console.aws.amazon.com/s3/home) data sources. It is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks.

Amazon S3 permissions required to run the connector:
  - ListAllMyBuckets
  - ListBucket
  - GetBucketLocation
  - GetObject

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Amazon S3 details](#gather-amazon-s3-details)
2. [Configure Amazon S3 connector](#configure-amazon-s3-connector)

#### Gather Amazon S3 details

Collect the information that is required to connect to your Amazon S3:

- Setup AWS configuration by installing [awscli](https://pypi.org/project/awscli/).
- Add aws_access_key, aws_secret_key and region to run the connector.


#### Configure Amazon S3 connector

The following configuration fields need to be provided for setting up the connector:

##### `buckets`

List buckets for Amazon S3. For * in string connector will fetch data for all the buckets. Examples:

  - `testbucket, prodbucket`
  - `testbucket`
  - `*`

##### `read_timeout`

The read_timeout for Amazon S3. Default value is `90`. Examples:

  - `60`
  - `120`

##### `connect_timeout`

The connect_timeout for crawling the Amazon S3. Default value is `90`. Examples:

  - `60`
  - `120`

##### `max_attempts`

The max_attempts for retry the Amazon S3. Default value is `5`. Examples:

  - `1`
  - `3`

##### `page_size`

The page_size for iterating bucket objects in Amazon S3. Default value is `100`. Examples:

  - `50`
  - `150`

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/s3.py#L25).

### Connector Limitations

- Files bigger than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.
- The user needs to set a profile with the AWS configure command.
- Currently the connector does not support S3 compatible vendors.

### E2E Tests

The framework provides a way to test ingestion through a connector against a real data source. This is called a functional test. To execute a functional test for the Amazon S3 connector, run the following command:
```shell
$ make ftest NAME=s3
```

ℹ️ Users do not need to have a running Elasticsearch instance or an Amazon S3 source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Amazon S3 source using the docker image.

ℹ️ The e2e test does not currently support testing ingestion against an Elastic Cloud instance. The test uses a dockerized Elasticsearch for the ingestion as configured [here](https://github.com/elastic/connectors-python/blob/8.6/connectors/sources/tests/fixtures/s3/docker-compose.yml).

ℹ️ The e2e test uses default values defined in [configure Amazon S3 connector](#configure-amazon-s3-connector)

## Google Cloud Storage Connector

The [Elastic Google Cloud Storage connector](https://github.com/elastic/connectors-python/blob/main/connectors/sources/google_cloud_storage.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.

The Google Cloud Storage service account must have (at least) the following scopes and roles :
  - Security Admin
  - Storage Admin
  - Serviceusage.services.use
  - Project Billing Manager

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Google Cloud Storage details](#gather-google-cloud-storage-details)
2. [Configure Google Cloud Storage connector](#configure-google-cloud-storage-connector)

#### Gather Google Cloud Storage details

Collect the information that is required to connect to your Google Cloud Storage:

- The Google Cloud Storage service account credentials json file.

#### Configure Google Cloud Storage connector

The following configuration fields need to be provided for setting up the connector:

##### `service_account_credentials`

The service account credentials generated from Google Cloud Storage.  Default value is [here](https://github.com/elastic/connectors-python/blob/main/connectors/sources/google_cloud_storage.py#L100)


##### `retry_count`

The number of retry attempts after failed call to Google Cloud Storage. Default value is `3`.

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/main/connectors/utils.py#L29).

### Connector Limitations

- Files bigger than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

### E2E Tests

The end to end test is usually performed by developers after the functional and system testing is completed. The framework allows users to test the connector end to end. To perform e2e test for Google Cloud Storage connector, run the following make command:
```shell
$ make ftest NAME=google_cloud_storage
```

ℹ️ Users do not need to have a running Elasticsearch instance or an Google Cloud Storage source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Google Cloud Storage source using the docker image.

ℹ️ The e2e test uses default values defined in [configure Google Cloud Storage connector](#configure-google-cloud-storage-connector).


## Azure Blob Storage Connector

The [Elastic Azure Blob Storage connector](https://github.com/elastic/connectors-python/blob/main/connectors/sources/azure_blob_storage.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Azure Blob Storage details](#gather-azure-blob-storage-details)
2. [Configure Azure Blob Storage connector](#configure-azure-blob-storage-connector)

#### Gather Azure Blob Storage details

Collect the information that is required to connect to your Azure Blob Storage:

- The account name and account key for the Azure Blob Storage account.
- The blob endpoint for Azure Blob Service.

#### Configure Azure Blob Storage connector

The following configuration fields need to be provided for setting up the connector:

##### `account_name`

The account name of Azure Blob Storage account. Default value is `devstoreaccount1`.

##### `account_key`

The account key of Azure Blob Storage account. Default value is `Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==`.

##### `blob_endpoint`

The blob endpoint of Blob Service provided by Azure Blob Storage. Default value is `http://127.0.0.1:10000/devstoreaccount1`.

##### `retry_count`

The number of retry attempts after failed call to Azure Blob Storage. Default value is `3`.

##### `concurrent_downloads`

The number of concurrent downloads for fetching content of Azure Blob Storage. Default value is `100`.

#### Content extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found [here](https://github.com/elastic/connectors-python/blob/main/connectors/utils.py#L29).

### Connector Limitations

- Currently, Lease data and Blob tier wonâ€™t be updated in the document, because the blob timestamp is not updated. Issue can be found [here](https://github.com/elastic/connectors-python/issues/289).
- Files bigger than 10 MB won't be extracted.
- Permission are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

### E2E Tests

The end to end test is usually performed by developers after the functional and system testing is completed. The framework allows users to test the connector end to end. To perform e2e test for Azure Blob Storage connector, run the following make command:
```shell
$ make ftest NAME=azure_blob_storage
```

ℹ️ Users do not need to have a running Elasticsearch instance or an Azure Blob Storage source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Azure Blob Storage source using the docker image.

ℹ️ The e2e test uses default values defined in [configure Azure Blob Storage connector](#configure-azure-blob-storage-connector)


## PostgreSQL Connector

The [Elastic PostgreSQL connector](../connectors/sources/postgresql.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.

PostgreSQL versions 11 to 15 are compatible with Elastic connector frameworks.

**Prerequisites**

- The Tables which user wants to index should be owned by a PostgreSQL user.
- PostgreSQL user needs superuser privilges to index all the tables of the database.
- User needs to set `track_commit_timestamp` to `on`. User will set `track_commit_timestamp` to `on` by executing `ALTER SYSTEM SET track_commit_timestamp = on;` query in PostgreSQL server.

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather PostgreSQL details](#gather-postgresql-details)
2. [Configure PostgreSQL connector](#configure-postgresql-connector)

#### Gather PostgreSQL details

Collect the information that is required to connect to your PostgreSQL server:

- The server host address where the PostgreSQL server is hosted.
- The port where the PostgreSQL server is hosted.
- The username the connector will use to log in to the PostgreSQL server.
- The password the connector will use to log in to the PostgreSQL server.
- The database name the connector will query.
- SSL certificate if you want to establish secured connections.

#### Configure PostgreSQL connector

The following configuration fields need to be provided for setting up the connector:

##### `host`

The server host address where the PostgreSQL is hosted. Default value is `127.0.0.1`. Examples:

  - `192.158.1.38`
  - `demo.instance.demo-region.demo.service.com`

##### `port`

The port where the PostgreSQL is hosted. Default value is `9090`. Examples:

  - `5432`
  - `9090`

##### `username`

The username of the account for PostgreSQL. Default value is `admin`.

##### `password`

The password of the account to be used for the PostgreSQL. Default value is `Password_123`.

##### `database`

Name of the PostgreSQL database. Default value is `xe`. Examples:

  - `employee_database`
  - `customer_database`

##### `tables`

Comma-separated list of tables. The PostgreSQL connector will fetch data from all tables present in the configured database, if the value is `*` . Default value is `*`. Examples:

  - `table_1, table_2`
  - `*`

##### `fetch_size`

The number of rows to fetch on each request to the PostgreSQL. Default value is `50`.

##### `retry_count`

The number of retry attempts after failed request to the PostgreSQL. Default value is `3`.

##### `ssl_enabled`

SSL verification should be enabled or not. Default value is `False`.

##### `ssl_ca`

Content of SSL certificate. If SSL is disabled, keep the `ssl_ca` field empty. Example certificate:

  - ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    AlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHROb2RlMDExFjAUBgNV
    BAsTDURlZmF1bHRDZWxsMDExGTAXBgNVBAsTEFJvb3QgQ2VydGlmaWNhdGUxEjAQ
    BgNVBAMTCWxvY2FsaG9zdDAeFw0yMTEyMTQyMjA3MTZaFw0yMjEyMTQyMjA3MTZa
    MF8xCzAJBgNVBAYTAlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHRO
    b2RlMDExFjAUBgNVBAsTDURlZmF1bHRDZWxsMDExEjAQBgNVBAMTCWxvY2FsaG9z
    dDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMv5HCsJZIpI5zCy+jXV
    z6lmzNc9UcVSEEHn86h6zT6pxuY90TYeAhlZ9hZ+SCKn4OQ4GoDRZhLPTkYDt+wW
    CV3NTIy9uCGUSJ6xjCKoxClJmgSQdg5m4HzwfY4ofoEZ5iZQ0Zmt62jGRWc0zuxj
    hegnM+eO2reBJYu6Ypa9RPJdYJsmn1RNnC74IDY8Y95qn+WZj//UALCpYfX41hko
    i7TWD9GKQO8SBmAxhjCDifOxVBokoxYrNdzESl0LXvnzEadeZTd9BfUtTaBHhx6t
    njqqCPrbTY+3jAbZFd4RiERPnhLVKMytw5ot506BhPrUtpr2lusbN5svNXjuLeea
    MMUCAwEAAaOBoDCBnTATBgNVHSMEDDAKgAhOatpLwvJFqjAdBgNVHSUEFjAUBggr
    BgEFBQcDAQYIKwYBBQUHAwIwVAYDVR0RBE0wS4E+UHJvZmlsZVVVSUQ6QXBwU3J2
    MDEtQkFTRS05MDkzMzJjMC1iNmFiLTQ2OTMtYWI5NC01Mjc1ZDI1MmFmNDiCCWxv
    Y2FsaG9zdDARBgNVHQ4ECgQITzqhA5sO8O4wDQYJKoZIhvcNAQELBQADggEBAKR0
    gY/BM69S6BDyWp5dxcpmZ9FS783FBbdUXjVtTkQno+oYURDrhCdsfTLYtqUlP4J4
    CHoskP+MwJjRIoKhPVQMv14Q4VC2J9coYXnePhFjE+6MaZbTjq9WaekGrpKkMaQA
    iQt5b67jo7y63CZKIo9yBvs7sxODQzDn3wZwyux2vPegXSaTHR/rop/s/mPk3YTS
    hQprs/IVtPoWU4/TsDN3gIlrAYGbcs29CAt5q9MfzkMmKsuDkTZD0ry42VjxjAmk
    xw23l/k8RoD1wRWaDVbgpjwSzt+kl+vJE/ip2w3h69eEZ9wbo6scRO5lCO2JM4Pr
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

ℹ️ Default values exist for end-to-end testing only.

### Connector Limitations

- To fetch the last updated time in PostgreSQL, `track_commit_timestamp` must be set to `on`. Otherwise, all data will be indexed in every sync.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently, the filtering is controlled via ingest pipelines.

### E2E Tests

The framework provides a way to test ingestion through a connector against a real data source. This is called a functional test. To execute a functional test for the PostgreSQL connector, run the following command:
```shell
$ make ftest NAME=postgresql
```

ℹ️ Users do not need to have a running Elasticsearch instance or a PostgreSQL source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock PostgreSQL source using the docker image.

ℹ️ The e2e test uses default values defined in [Configure PostgreSQL connector](#configure-postgresql-connector)


## Oracle Database Connector

The [Elastic Oracle Database connector](../connectors/sources/oracle.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.

Oracle Database versions 18c, 19c and 21c are compatible with Elastic connector frameworks.

**Prerequisites**

- The database user requires `CONNECT` and `DBA` privileges and must be the owner of the tables to be indexed.

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Oracle Database details](#gather-oracle-database-details)
2. [Configure Oracle Database connector](#configure-oracle-database-connector)

#### Gather Oracle Database details

Collect the information that is required to connect to your Oracle Database:

- The server host address where the Oracle Database is hosted.
- The port where the Oracle Database is hosted.
- The username the connector will use to log in to the Oracle Database.
- The password the connector will use to log in to the Oracle Database.
- The SID of Oracle Database where the connector will query data.

#### Configure Oracle Database connector

The following configuration fields need to be provided for setting up the connector:

##### `host`

The server host address where the Oracle Database is hosted. Default value is `127.0.0.1`. Examples:

  - `192.158.1.38`
  - `demo.instance.demo-region.demo.service.com`

##### `port`

The port where the Oracle Database is hosted. Default value is `9090`. Note: For a secured connection, a user needs to configure a SSL port.

  - `5432`
  - `9090`

    - ##### `oracle_protocol`

      The protocol which the connector uses to establish a connection. Default value is `TCP`. Note: For a secured connection, a user needs to use `TCPS`.

    - ##### `oracle_home`

      Path of the Oracle home directory to run connector with thick mode for secured connection. By default connector will run on Thin Mode. Note: In case of unsecured connection, keep `oracle_home` field empty.

    - ##### `wallet_configuration_path`

      Path of the oracle configuration files. Only applicable when configuration files are not at the default location. By default files located at `$ORACLE_HOME/network/admin`. Note: If Wallet configuration files are located at default location, keep `wallet_configuration_path` field empty.

##### `username`

The username of the account for the Oracle Database. Default value is `admin`.

##### `password`

The password of the account to be used for the Oracle Database. Default value is `Password_123`.

##### `database`

The SID of Oracle Database. Default value is `xe`. Examples:

  - `employee_database`
  - `customer_database`

##### `tables`

Comma-separated list of tables. The Oracle Database connector will fetch data from all tables present in the configured database, if the value is `*`. Default value is `*`. Examples:

  - `table_1, table_2`
  - `*`

##### `fetch_size`

Number of rows to fetch on each request to Oracle Database. Default value is `50`.

##### `retry_count`

The number of retry attempts after failed request to Oracle Database. Default value is `3`.

ℹ️ Default values exist for end-to-end testing only.

### Setup for a secured connection with Oracle
 - User needs to install the Oracle service on the system where the connector is running.
 - Set the `oracle_home` parameter to the location of your Oracle home directory. If configuration files are not at the default location, set `wallet_configuration_path` parameter.
  - Now create a directory to store the wallet.
```shell
$ mkdir $ORACLE_HOME/ssl_wallet
```
  - User needs to create `sqlnet.ora` at `$ORACLE_HOME/network/admin` location and add the following content.
```
WALLET_LOCATION = (SOURCE = (METHOD = FILE) (METHOD_DATA = (DIRECTORY = $ORACLE_HOME/ssl_wallet))) 
SSL_CLIENT_AUTHENTICATION = FALSE 
SSL_VERSION = 1.0 
SSL_CIPHER_SUITES = (SSL_RSA_WITH_AES_256_CBC_SHA) 
SSL_SERVER_DN_MATCH = ON
```
  - Then execute the following commands to create a wallet and attach an SSL certificate.
```shell
$ orapki wallet create -wallet path-to-oracle-home/ssl_wallet -auto_login_only

$ orapki wallet add -wallet path-to-oracle-home/ssl_wallet -trusted_cert -cert path-to-oracle-home/ssl_wallet/root_ca.pem -auto_login_only
```
Replace the file name with your file.

ℹ️ For better understanding, please refer following Amazon RDS [documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.Options.SSL.html). It is well documented.


### Connector Limitations

- If the value of the table's system change number (SCN) is not between the `min(SCN)` and `max(SCN)` values of the `SMON_SCN_TIME` table, the connector will not be able to retrieve the most recently updated time, and data will be indexed in every sync. For more details check out the following [documentation](https://community.oracle.com/tech/apps-infra/discussion/4076446/show-error-about-ora-08181-specified-number-is-not-a-valid-system-change-number-when-using-scn-t).
- The Oracle Database connector does not allow the `sys` user as it contains 1000+ system tables which are irrelevant for users. If the user is working with `sys` user, the user needs to pass either `sysdba` or `sysoper` mode.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently, the filtering is controlled via ingest pipelines.

### E2E Tests

The framework provides a way to test ingestion through a connector against a real data source. This is called a functional test. To execute a functional test for the Oracle connector, run the following command:
```shell
$ make ftest NAME=oracle
```

ℹ️ Users do not need to have a running Elasticsearch instance or an Oracle source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Oracle source using the docker image.

ℹ️ The e2e test uses default values defined in [Configure Oracle Database connector](#configure-oracle-database-connector)


## Microsoft SQL Connector

The [Elastic Microsoft SQL connector](../connectors/sources/mssql.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

### Availability and prerequisites

⚠️ _Currently, this connector is available in **beta** starting in 8.7_.
Features in beta are subject to change and are not covered by the service level agreement (SLA) of features that have reached general availability (GA).

Elastic versions 8.6.0+ are compatible with Elastic connector frameworks. Your deployment must include the Elasticsearch, Kibana, and Enterprise Search services.

Microsoft SQL Server versions 2017, 2019, and Azure SQL, Amazon RDS for Microsoft SQL Server are compatible with Elastic connector frameworks.

**Prerequisites**

- The Microsoft SQL connector requires an ODBC driver to execute. Connector supports `Microsoft ODBC Driver 18`. To install ODBC driver please refer following [documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-driver-manager).
- Grant user with `sysadmin` server role.

### Setup and basic usage

Complete the following steps to deploy the connector:

1. [Gather Microsoft SQL details](#gather-microsoft-sql-details)
2. [Configure Microsoft SQL connector](#configure-microsoft-sql-connector)

#### Gather Microsoft SQL details

Collect the information that is required to connect to your Microsoft SQL Server:

- The server host address where the Microsoft SQL Server is hosted.
- The port where the Microsoft SQL Server is hosted.
- The username the connector will use to log in to the Microsoft SQL Server.
- The password the connector will use to log in to the Microsoft SQL Server.
- The database name where the connector will query data.

#### Configure Microsoft SQL connector

The following configuration fields need to be provided for setting up the connector:

##### `host`

The server host address where the Microsoft SQL Server is hosted. Default value is `127.0.0.1`. Examples:

  - `192.158.1.38`
  - `demo.instance.demo-region.demo.service.com`

##### `port`

The port where the Microsoft SQL Server is hosted. Default value is `9090`. Examples:

  - `5432`
  - `9090`

##### `username`

The username of the account for Microsoft SQL Server. Default value is `admin`.

##### `password`

The password of the account to be used for the Microsoft SQL Server. Default value is `Password_123`.

##### `database`

Name of the Microsoft SQL Server database. Default value is `xe`. Examples:

  - `employee_database`
  - `customer_database`

##### `tables`

Comma-separated list of tables. The Microsoft SQL connector will fetch data from all tables present in the configured database, if the value is `*` . Default value is `*`. Examples:

  - `table_1, table_2`
  - `*`

##### `fetch_size`

The number of rows to fetch on each request to Microsoft SQL Server. Default value is `50`.

##### `retry_count`

The number of retry attempts after failed request to Microsoft SQL Server. Default value is `3`.

##### `mssql_driver`

Name of Microsoft ODBC Driver for SQL Server. User finds the name of the installed driver in `odbcinst.ini` file. To find the location of the `odbcinst.ini` file execute `odbcinst -j` command. Default value is `ODBC Driver 18 for SQL Server`.

##### `secured_connection`

Whether the connector should establish a secured connection. Default value is `False`.

ℹ️ For secured connection Microsoft ODBC Driver for SQL Server uses the OpenSSL library, which gets a certificate from the Default Certificate Trust Store of OpenSSL. For more details check out the following [documentation](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/connection-string-keywords-and-data-source-names-dsns?view=sql-server-ver16#using-tlsssl)

ℹ️ Default values exist for end-to-end testing only.

### Connector Limitations

- If the `last_user_update` of `sys.dm_db_index_usage_stats` table for specific table and database is not available then all data in that table will be synced.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently, the filtering is controlled via ingest pipelines.

### E2E Tests

The framework provides a way to test ingestion through a connector against a real data source. This is called a functional test. To execute a functional test for the Microsoft SQL connector, run the following command:
```shell
$ make ftest NAME=mssql
```

ℹ️ Users do not need to have a running Elasticsearch instance or a mssql source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Microsoft SQL source using the docker image.

ℹ️ The e2e test uses default values defined in [Configure Microsoft SQL connector](#configure-microsoft-sql-connector)

## General Configuration

The details of Elastic instance and other relevant fields such as `service` and `source` needs to be provided in the [config.yml](https://github.com/elastic/connectors-python/blob/8.6/config.yml) file. For more details check out the following [documentation](https://github.com/elastic/connectors-python/blob/8.6/docs/CONFIG.md).

## Installation

Provides a CLI to ingest documents into Elasticsearch, following the [connector protocol](https://github.com/elastic/connectors-python/blob/main/docs/CONNECTOR_PROTOCOL.md).

To install the CLI, run:
```shell
$ make install
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

# Architecture

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

# Sync strategy

In Workplace Search we have the four following syncs:

- **Full sync** (runs every 72 hours by default): This synchronization job synchronizes all the data from the content source ensuring full data parity.
- **Incremental sync** (runs every 2 hours by default): This synchronization job synchronizes updates to the data at the content source ensuring high data freshness.
- **Deletion sync** (runs every 6 hours by default): This synchronization job synchronizes document deletions from the content source ensuring regular removal of stale data.
- **Permissions sync** (runs every 5 minutes by default, when Document Level Permissions are enabled): This synchronization job synchronizes document permissions from the content sources ensuring secure access to documents on Workplace Search.

In Elastic Python connectors we are implementing for now just **Full sync**, which ensures full data parity (including deletion).

This sync strategy is good enough for some sources like MongoDB where 100,000 documents can be fully synced in less than 30 seconds.

We will introduce more sophisticated syncs as we add new sources, in order to achieve the same level of freshness we have in Workplace Search.

The **Permissions sync** will be included later as well once we have designed how Document-Level Permission works in the new architecture.

# How a sync works

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

## Runtime dependencies

- MacOS or Linux server. The connector has been tested with CentOS 7, MacOS Monterey v12.0.1.
- Python version 3.10 or later.
- To fix SSL certificate verification failed error, users have to run this to connect with Amazon S3:
    ```shell
    $ System/Volumes/Data/Applications/Python\ 3.10/Install\ Certificates.command
    ```
