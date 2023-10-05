# Box Connector

The [Elastic Box connector](../connectors/sources/box.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Box** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Box API Authorization

### Box Free Account

#### Create Box User Authentication (OAuth 2.0) Custom App

You'll need to create an OAuth app in the Box developer console by following these steps:

1. Register a new app in the [Box dev console](https://app.box.com/developers/console) with custom App and select User authentication (OAuth 2.0).

2. Add your Elasticsearch endpoint URL to **Redirect URIs**.

3. Check "Write all files and folders stored in Box" in Application Scopes.

4. Once the app is created, **Client ID** and **Client secret** values are available in the configuration tab. Keep these handy.

#### Generate a refresh Token

To generate a refresh token, follow these steps:

1. Go to the following URL, replacing `<CLIENT_ID>` with the **Client ID** value saved earlier.
    ```shell
    https://account.box.com/api/oauth2/authorize?response_type=code&client_id=<CLIENT_ID>
    ```
    
The HTTP response should contain an **authorization code** that you'll use to generate a refresh token.

**Note:** Authorization codes to generate refresh tokens can only be used once and are only valid for 30 seconds.

2. In your terminal, run the following `curl` command, replacing `<AUTHORIZATION_CODE>`, `<CLIENT_ID> and <CLIENT_SECRET>` with the values you saved earlier:
    ```shell
    curl -i -X POST "https://api.box.com/oauth2/token" \
        -H "Content-Type: application/x-www-form-urlencoded" \
        -d "client_id=<CLIENT_ID>" \
        -d "client_secret=<CLIENT_SECRET>" \
        -d "code=<AUTHORIZATION_CODE>" \
        -d "grant_type=authorization_code"
    ```
Save the refresh token from the response. You'll need this for the connector configuration.

### Box Enterprise Account

#### Create Box Server Authentication (Client Credentials Grant) Custom App

1. Register a new app in the [Box dev console](https://app.box.com/developers/console) with custom App and select Server Authentication (Client Credentials Grant).

2. Check following permissions:
    - "Write all files and folders stored in Box" in Application Scopes
    - "Make API calls using the as-user header" in Advanced Features

3. Select `App + Enterprise Access` in App Access Level.

4. Authorize your application from the admin console.

Save the **Client Credentials** and **Enterprise ID**. You'll need these to configure the connector.

## Configuration

### Configure Box connector

#### `Box Account`  (required)

Dropdown to determine Box Account type: `Box Free Account` or `Box Enterprise Account`. Default value is `Box Free Account`.

#### `Client ID`  (required)

The Client ID to authenticate with Box instance.

#### `Client Secret`  (required)

The Client Secret to authenticate with Box instance.

#### `Refresh Token` (required if Box Account is Box Free)

The Refresh Token to generate Access Token. 

**NOTE:** If the process terminates, you'll need to generate a new refresh token.

#### `Enterprise ID`  (required if Box Account is Box Enterprise)

The Enterprise ID to authenticate with Box instance.

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Files**
- **Folders**

*NOTE*:
- Files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced sync rules are not available for this connector in the present version.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for Box connector, run the following command:

```shell
$ make ftest NAME=box
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Box source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Box source using the docker image.

### General

- Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
