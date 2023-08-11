# Dropbox Connector

The [Elastic Dropbox connector](../../connectors/sources/dropbox.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Dropbox** tile from the connectors list or **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

Creating a Dropbox application is required to help the Dropbox connector to authenticate. Generating a refresh token is a required step to configure the connector.

## Dropbox API Authorization

### Create Dropbox OAuth App

You'll need to create an OAuth app in the Dropbox platform by following these steps:
1. Register a new app in the [Dropbox App Console](https://www.dropbox.com/developers/apps). Select **Full Dropbox API app** and choose the following required permissions:
    - files.content.read
    - sharing.read

2. Once the app is created, make note of the **app key** and **app secret** values which you'll need to configure the Dropbox connector on your Elastic deployment.

### Generate a refresh Token

To generate a refresh token, follow these steps:
1. Go to the following URL, replacing `<APP_KEY>` with the **app key** value saved earlier.
    ```shell
    https://www.dropbox.com/oauth2/authorize?client_id=<APP_KEY>&response_type=code&token_access_type=offline
    ```
    
The HTTP response should contain an **authorization code** that you'll use to generate a refresh token.

**Note:** An authorization code can only be used once to create a refresh token.

2. In your terminal, run the following `cURL` command, replacing `<AUTHORIZATION_CODE>`, `<APP_KEY>:<APP_SECRET>` with the values you saved earlier:
    ```shell
    curl -X POST "https://api.dropboxapi.com/oauth2/token?code=<AUTHORIZATION_CODE>&grant_type=authorization_code" -u "<APP_KEY>:<APP_SECRET>"
    ```
Store the refresh token from the response and use it in the connector configuration.
**Note:** Make sure the response has a similar list of following scopes:
   - `account_info.read`
   - `files.content.read`
   - `files.metadata.read`
   - `sharing.read`

## Configuration

### Configure Dropbox connector

The following configuration fields are required to set up the connector:

#### `path`

The folder path to fetch files/folders from Dropbox. Default value is `/`.

#### `app_key` (required)

The App Key to authenticate your Dropbox application.

#### `app_secret` (required)

The App Secret to authenticate your Dropbox application.

#### `refresh_token` (required)

The refresh token to authenticate your Dropbox application.

#### `retry_count`

The number of retry attempts after a failed request to Dropbox. Default value is `3`.

#### `concurrent_downloads`

The number of concurrent downloads for fetching attachment content. 
This can help speed up content extraction of attachments. Defaults to `100`.

### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../../connectors/utils.py) file.

## Documents and syncs

The connector syncs the following objects and entities:
- **Files**
    - Includes metadata such as file name, path, size, content, etc.
- **Folders**

**NOTE**:
- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/8.9/sync-rules.html#sync-rules-basic "Basic sync rules") are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.

## Advanced Sync Rules

Advanced Sync Rules are available as of Elastic version 8.10.

- Users can add [Dropbox query](https://www.dropbox.com/developers/documentation/http/documentation#files-search) results to content being indexed.
    - Test search responses using the [Dropbox API Explorer](https://dropbox.github.io/dropbox-api-v2-explorer/#files_search_v2).
- All data returned by queries will be indexed.
- You can refer to [DROPBOX.md](../connectors/docs/sync-rules/DROPBOX.md) for the correct format to add sync rules. Any errors encountered will be reported in the sync rules overview.

**Note:** The "path" configuration field will be overridden by the advanced rules.

## Connector Client operations

### End-to-end Testing

End-to-end testing is not available for this connector in this version.

## Known issues

- There is an issue where metadata updates to Paper files from Dropbox Paper application are not immediately reflected in the Dropbox UI, which delays the availability of updated results for the connector. Once the metadata changes are visible in the Dropbox UI, the updates are available.

Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

