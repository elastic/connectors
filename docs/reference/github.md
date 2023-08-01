# GitHub Connector

The [Elastic GitHub connector](../connectors/sources/github.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **GitHub** tile from the connectors list or **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## GitHub access token

Configure a GitHub access token to fetch data from GitHub. 

Follow these steps to generate a GitHub access token:
 - Go to GitHub Settings → Developer settings → Personal access tokens → Tokens(classic).
 - Click `Generate new token`.
 - Add a note and select the "repo" scope.
 - Click `Generate token` and copy the token.

## Configuration

### Configure GitHub connector

The following configuration fields are required:

#### `data_source`

Dropdown to determine GitHub platform type: `GitHub Cloud` or `GitHub Server`. Default value is `GitHub Server`.

#### `host`

The domain where GitHub is hosted. Examples:

- `https://test-server.github.dev`

#### `token`

GitHub access token to authenticate the GitHub instance.

#### `repositories`

Comma-separated list of repositories to fetch data from GitHub instance. If the value is `*` the connector will fetch data from all repositories present in the configured user's account. Default value is `*`. Examples:

- `Repository1, Repository2`
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

The number of retry attempts after failed request to GitHub. Default value is `3`.

### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be `.md`, `.markdown`, `.rst`.

## Documents and Sync

The connector syncs the following objects and entities:
- **Repositories**
- **Pull Requests**
- **Issues**
- **Markdown & README Files**

*NOTE*:
- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced Sync Rules are available in 8.10+.

- Users can add [Github queries](https://docs.github.com/en/search-github/searching-on-github/searching-issues-and-pull-requests) for syncing the source.
- All the data returned by queries will be indexed.
- You can refer to [GITHUB.md](../connectors/docs/sync-rules/GITHUB.md) for the correct format to add sync rules. Any errors encountered, will be presented to the user visually in the sync rules overview.

**Note:** "List of repositories" configuration field will be overridden by the advanced rules.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for GitHub connector, run the following command:

```shell
$ make ftest NAME=github
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a GitHub source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock GitHub source using the docker image.

## Known issues

- There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
