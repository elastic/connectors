# Jira Connector

The [Elastic Jira connector](../connectors/sources/jira.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**.  This connector is in **beta** and is subject to change. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Compatibility

Jira Cloud or Jira Server versions 7 or later are compatible with Elastic connector frameworks. Jira Data Center editions are not currently supported.

## Configuration

### Configure Jira connector

The following configuration fields need to be provided for setting up the connector:

#### `data_source`

Dropdown to determine Jira platform type: `Jira Cloud` or `Jira Server`. Default value is `Jira Cloud`.

#### `username`

The username of the account for Jira server.

#### `password`

The password of the account to be used for Jira server.

#### `account_email`

The account email for Jira cloud.

#### `api_token`

The API Token to authenticate with Jira cloud.

#### `jira_url`

The domain where Jira is hosted. Examples:

- `https://192.158.1.38:8080/`
- `https://test_user.atlassian.net/`

#### `projects`

Comma-separated list of [Project Keys](https://support.atlassian.com/jira-software-cloud/docs/what-is-an-issue/#Workingwithissues-Projectkeys) to fetch data from Jira server or cloud. If the value is `*` the connector will fetch data from all projects present in the configured `projects` . Default value is `*`. Examples:

- `EC, TP`
- `*`

#### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

#### `ssl_ca`

Content of SSL certificate. Note: In case of ssl_enabled is `False`, keep `ssl_ca` field empty. Example certificate:

- ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```

#### `retry_count`

The number of retry attempts after failed request to Jira. Default value is `3`.

#### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `100`.

### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.

## Documents and Sync

The connector syncs the following objects and entities:
- **Projects**
    - Includes metadata such as description, project key, project type, lead name, etc.
- **Issues**
    - All types of issues including Task, Bug, Sub-task, Enhancement, Story, etc.
    - Includes metadata such as issue type, parent issue details, fix versions, affected versions, resolution, attachments, comments, sub-task details, priority, custom fields, etc.
- **Attachments**

**Note:** Archived projects and issues are not indexed.

## Sync rules

- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Jira connector, run the following command:

```shell
$ make ftest NAME=jira
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Jira source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Jira source using the docker image.

## Known issues

There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

