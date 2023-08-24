# OneDrive Connector

The [Elastic OneDrive connector](../connectors/sources/onedrive.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **OneDrive** tile from the connectors list **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to OneDrive

To connect to OneDrive you need to [create an Azure Active Directory application and service principal](https://learn.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) that can access resources. Follow these steps:

1. Go to the Azure portal (https://portal.azure.com) and sign in with your Azure account.

2. Navigate to the "Azure Active Directory" service.

3. Select "App registrations" from the left-hand menu.	

4. Click on the "New registration" button to register a new application.

5. Provide a name for your app, and optionally select the supported account types (e.g., single tenant, multi-tenant).

6. Click on the "Register" button to create the app registration.

7. After the registration is complete, you will be redirected to the app's overview page. Take note of the "Application (client) ID" value, as you'll need it later.

8. Scroll down to the "API permissions" section and click on the "Add a permission" button.

9. In the "Request API permissions" pane, select "Microsoft Graph" as the API.

10. Choose the application permissions and select the following permissions under the "Application" tab: `User.Read.All`, `File.Read.All`

11. Click on the "Add permissions" button to add the selected permissions to your app.

Finally, click on the "Grant admin consent" button to grant the required permissions to the app. This step requires administrative privileges. ℹ️ **NOTE**: If you are not an admin, you need to request the Admin to grant consent via their Azure Portal.

12. Click on "Certificates & Secrets" tab. Go to Client Secrets. Generate a new client secret and keep a note of the string present under `Value` column.

After completion, use the following configuration parameters to configure the connector.

## Configuration

### Configure OneDrive connector

The following configuration fields are required:

#### `Azure application Client ID`  (required)

Unique identifier for your Azure Application, found on the app's overview page. Example:

- `ab123453-12a2-100a-1123-93fd09d67394`

#### `Azure application Client Secret`  (required)

String value that the application uses to prove its identity when requesting a token, available under the `Certificates & Secrets` tab of your Azure application menu. Example:

- `eyav1~12aBadIg6SL-STDfg102eBfCGkbKBq_Ddyu`

#### `Azure application Tenant ID`  (required)

It is a unique identifier of your Azure Active Directory instance. Example:

- `123a1b23-12a3-45b6-7c8d-fc931cfb448d`

#### `Maximum retries per request`

The number of retry attempts after failed request to OneDrive. Default value is `3`.

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Files**
    - Includes metadata such as file name, path, size, content, etc.
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

To perform E2E testing for OneDrive connector, run the following command:

```shell
$ make ftest NAME=onedrive
```

ℹ️ Users can generate the performance report using an argument i.e. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a OneDrive source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock OneDrive source using the docker image.

## Known issues

- There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
