# Microsoft Teams Connector

The [Elastic Microsoft Teams connector](../connectors/sources/microsoft_teams.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Microsoft Teams** tile from the connectors list **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to Microsoft Teams

To connect to Microsoft Teams you need to [create an Azure Active Directory application and service principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) that can access resources. Follow these steps:

1. Go to the Azure portal (https://portal.azure.com) and sign in with your Azure account.

2. Navigate to the "Azure Active Directory" service.

3. Select "App registrations" from the left-hand menu.	

4. Click on the "New registration" button to register a new application.

5. Provide a name for your app, and optionally select the supported account types (e.g., single tenant, multi-tenant).

6. Click on the "Register" button to create the app registration.

7. After the registration is complete, you will be redirected to the app's overview page. Take note of the "Application (client) ID" value, as you'll need it later.

8. Scroll down to the "API permissions" section and click on the "Add a permission" button.

9. In the "Request API permissions" pane, select "Microsoft Graph" as the API.

10. Choose the permissions and select the following permissions:
    - User.Read.All (Delegated and Application)
    - TeamMember.Read.All (Delegated)
    - Team.ReadBasic.All (Delegated)
    - TeamsTab.Read.All (Delegated)
    - Group.Read.All (Delegated)
    - ChannelMessage.Read.All (Delegated)
    - Chat.Read (Delegated) & Chat.Read.All (Application)
    - Chat.ReadBasic (Delegated) & Chat.ReadBasic.All (Application)
    - Files.Read.All (Delegated and Application)
    - Calendars.Read (Delegated and Application)


11. Click on the "Add permissions" button to add the selected permissions to your app.

Click on the "Grant admin consent" button to grant the required permissions to the app. This step requires administrative privileges. ℹ️ **NOTE**: If you are not an admin, you need to request the admin to grant consent via their Azure Portal.

12. Click on "Certificates & Secrets" tab. Go to Client Secrets. Generate a new client secret and keep a note of the string present under `Value` column.

After completion, use the following configuration parameters to configure the connector.

## Configuration

### Configure Microsoft Teams connector

The following configuration fields are required:

#### `client_id`  (required)

Unique identifier for your Azure Application, found on the app's overview page. Example:

- `ab123453-12a2-100a-1123-93fd09d67394`

#### `secret_value`  (required)

String value that the application uses to prove its identity when requesting a token, available under the `Certificates & Secrets` tab of your Azure application menu. Example:

- `eyav1~12aBadIg6SL-STDfg102eBfCGkbKBq_Ddyu`

#### `tenant_id`  (required)

It is a unique identifier of your Azure Active Directory instance, found on the app's overview page. Example:

- `123a1b23-12a3-45b6-7c8d-fc931cfb448d`

#### `username`  (required)

Username for your Azure Application. Example:

- `dummy@3hmr2@onmicrosoft.com`

#### `password`  (required)

Password for your Azure Application. Example:

- `changeme`

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **USER_CHATS_MESSAGE**
- **USER_CHAT_TABS**
- **USER_CHAT_ATTACHMENT**
- **USER_CHAT_MEETING_RECORDING**
- **USER_MEETING**
- **TEAMS**
- **TEAM_CHANNEL**
- **CHANNEL_TAB**
- **CHANNEL_MESSAGE**
- **CHANNEL_MEETING**
- **CHANNEL_ATTACHMENT**
- **CALENDAR_EVENTS**


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

To perform E2E testing for Microsoft Teams connector, run the following command:

```shell
$ make ftest NAME=microsoft_teams
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Microsoft Teams source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Microsoft Teams source using the docker image.

## Known issues

- There is an issue where messages of oneOnOne chat for self user are not fetched in Graph APIs. Hence it won't be indexed in Elasticsearch.

Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
