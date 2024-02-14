# Notion Connector

The [Elastic Notion connector](../connectors/sources/notion.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, select the **Notion** tile when creating a new connector under **Search -> Connectors**.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to Notion

To connect to Notion, the user needs to [create an internal integration](https://www.notion.so/help/create-integrations-with-the-notion-api#create-an-internal-integration) for their Notion workspace, which can access resources using the Internal Integration Secret Token. Configure the Integration with following settings:

1. Users must grant `READ` permission for content, comment and user capabilities for that integration from the Capabilities tab.

2. Users must manually [add the integration as a connection](https://www.notion.so/help/add-and-manage-connections-with-the-api#add-connections-to-pages) to each page and database that needs to be retrieved.

Once completed, use the following parameters to configure the connector.

## Configuration

### Configure Notion connector

Note the following configuration fields:

#### `Notion Secret Key`  (required)

Secret token assigned to your integration, for a particular workspace. Example:

- `zyx-123453-12a2-100a-1123-93fd09d67394`

#### `Databases`  (required)

Comma-separated list of database names to be fetched by the connector. If the value is `*`, connector will fetch all the databases available in the workspace. Example:

- `database1, database2`
- `*`

#### `Pages`  (required)

Comma-separated list of page names to be fetched by the connector. If the value is `*`, connector will fetch all the pages available in the workspace. Examples:

- `*`
- `Page1, Page2`

#### `Index Comments`

Toggle to enable fetching and indexing of comments from the Notion workspace for the configured pages, databases and the corresponding child blocks. Default value is `False`.

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Pages**
    - Includes metadata such as page name, id, last updated time, etc.
- **Blocks**
    - Includes metadata such as title, type, id, content (in case of file block), etc.
- **Databases**
    - Includes metadata such as name, id, records, size, etc.
- **Users**
    - Includes metadata such as name, id, email address, etc.
- **Comments**
    - Includes the content and metadata such as id, last updated time, created by, etc.

*NOTE*:
- Files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to the destination Elasticsearch index.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced sync rules are not available for this connector in the present version.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Notion connector, run the following command:

```shell
$ make ftest NAME=notion
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Notion source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Notion source using the docker image.

## Known issues

- There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
