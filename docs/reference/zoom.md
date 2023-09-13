# Zoom Connector

The [Elastic Zoom connector](../connectors/sources/zoom.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Zoom** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to Zoom

To connect to Zoom you need to [create an Server-to-Server OAuth application](https://developers.zoom.us/docs/internal-apps/s2s-oauth/) that can access resources. Follow these steps:

1. Go to the Zoom App Marketplace (https://marketplace.zoom.us/) and sign in with your Zoom account.

2. Navigate to the "Develop" service.

3. Select "Build App" from the dropdown menu.	

4. Click on the "Server-to-Server OAuth" button to register a new application.

5. Provide a name for your app.

6. Click on the "Create" button to create the app registration.

7. After the registration is complete, you will be redirected to the app's overview page. Take note of the "App Credentials" value, as you'll need it later.

8. Navigate to the "Scopes" section and click on the "Add Scopes" button.

9. The following scopes need to be added to the app.

```shell
user:read:admin
meeting:read:admin
chat_channel:read:admin
recording:read:admin
chat_message:read:admin
report:read:admin
```

10. Click on the "Done" button to add the selected scopes to your app.

11. Navigate to the "Activation" section and input the necessary information to activate the app.

After completion, use the following configuration parameters to configure the connector.

## Configuration

### Configure Zoom connector

The following configuration fields are required:

#### `Zoom application Account ID`  (required)

The "Account ID" is a unique identifier associated with a specific Zoom account within the Zoom platform, found on the app's overview page. Example:

- `KVx-aQssTOutOAGrDfgMaA`

#### `Zoom application Client ID`  (required)

The "Client ID" refers to a unique identifier associated with an application that integrates with the Zoom platform, found on the app's overview page. Example:

- `49Z69_rnRiaF4JYyfHusw`

#### `Zoom application Client Secret`  (required)

The "Client Secret" refers to a confidential piece of information generated when developers register an application on the Zoom Developer Portal for integration with the Zoom platform, found on the app's overview page. Example:

- `eieiUJRsiH543P5NbYadavczjkqgdRTw`

#### `Recording Age Limit (Months)`  (required)

How far back in time to request recordings from zoom. Recordings older than this will not be indexed. This configuration parameter allows you to define a time limit, measured in months, for which recordings will be indexed.

#### `Fetch past meeting details`

Retrieve more information about previous meetings, including their details and participants. Default value is `False`. Enable this option to fetch past meeting details. This setting can increase sync time.

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Users**
- **Live Meetings**
- **Upcoming Meetings**
- **Past Meetings**
- **Recordings**
- **Channels**
- **Chat Messages**
- **Chat Files**

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

To perform E2E testing for Zoom connector, run the following command:

```shell
$ make ftest NAME=zoom
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Zoom source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Zoom source using the docker image.

## Known issues

- *Meetings* - Users can only index meetings that are less than a month old.
- *Chat Messages & Files* - Users can only index chats and files that are less than 6 months old.

### General 
- Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
