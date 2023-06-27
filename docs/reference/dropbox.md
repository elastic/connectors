# Dropbox Connector

The [Elastic Dropbox connector](../connectors/sources/dropbox.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

Creating a Dropbox application is required to help the Dropbox connector to authenticate.

## Dropbox API Authorization

### Create Dropbox OAuth App

To generate App Key and App Secret, follow the next steps:
1. Register a new app in the [App Console](https://www.dropbox.com/developers/apps) and select Full Dropbox API app and choose the following required permissions.
    - files.content.read
    - sharing.read

2. Once the app is created, please store **app key** and **app secret** for later configuration use.

### Generate a Refresh Token

To generate a refresh token, follow the next steps:
1. Hit the following URL with the generated APP_KEY on your browser:
    ```shell
    https://www.dropbox.com/oauth2/authorize?client_id=<APP_KEY>&response_type=code&token_access_type=offline
    ```
    
    In the HTTP response, you will get an **authorization code** that will be used to generate a refresh token.

    **Note:** Authorization code can only be used once to create a refresh token.

2. Perform the following POST API call in the terminal:
    ```shell
    curl -X POST "https://api.dropboxapi.com/oauth2/token?code=<AUTHORIZATION_CODE>&grant_type=authorization_code" -u "<APP_KEY>:<APP_SECRET>"
    ```
    Store the refresh token from the response and use it in the connector configuration.
    **Note:** Make sure the response has a similar list of following scopes:
    - account_info.read
    - files.content.read
    - files.metadata.read
    - sharing.read

## Configuration

### Configure Dropbox connector

The following configuration fields need to be provided for setting up the connector:

#### `path`

The folder path to fetch files/folders for Dropbox connector. Default value is `/`.

#### `app_key`

The App Key to authenticate the Dropbox application.

#### `app_secret`

The App Secret to authenticate the Dropbox application.

#### `refresh_token`

The Refresh Token to authenticate the Dropbox application.

#### `retry_count`

The number of retry attempts after failed request to Dropbox. Default value is `3`.

#### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This might speed up the content extraction of attachments. Defaults to `100`.

### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.

## Documents and Sync

The connector syncs the following objects and entities:
- **Files**
    - Includes metadata such as file name, path, size, content, etc.
- **Folders**

## Sync rules

- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Connector Client operations

### End-to-end Testing

End-to-end is not a part of the current version of the connector.

## Known issues

- There is a delay in the updated metadata of paper files to be reflected in the UI hence response does not have updated results. As soon as the change is reflected in the UI, the same is available in response.

Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

