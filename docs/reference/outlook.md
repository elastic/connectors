# Outlook Connector

The [Elastic Outlook connector](../connectors/sources/outlook.py) is built with the Elastic connectors Python framework and is available as a self-managed [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Outlook** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connecting to Outlook

### Connect to Exchange Server

To order to connect to Outlook, the connector fetches Active Directory users with the help of `ldap3` python library.

### Connect to Office365 Outlook

To order to connect to Outlook, user needs to create an Azure application as follows:

1. Log into Microsoft [Azure Portal](https://portal.azure.com) and click on App registrations and register an application.
2. From Overview tab, keep `Client ID` and `Tenant ID` handy.
3. Go to **Certificates & secrets** and create a new client secret.
4. Before configuring `Client ID`, `Tenant ID` and `Client Secret` in elastic, from API permissions tab - add below permission to fetch the Outlook content,
    - Add permissions -> APIs my organization uses -> Office 365 Exchange Online -> full_access_as_app (Application permission)
5. Now, user can configure `Client ID`, `Tenant ID` and `Client Secret` values to Outlook connector.


## Configuration

### Configure Outlook connector

#### `Outlook data source`  (required)

Dropdown to determine Outlook platform type: `outlook_cloud` or `outlook_server`. Default value is `outlook_cloud`.

#### `Tenant ID`  (required if data source is outlook_cloud)

The Tenant ID for the Azure account hosting the Outlook instance.

#### `Client ID`  (required if data source is outlook_cloud)

The Client ID to authenticate with Outlook instance.

#### `Client Secret Value`  (required if data source is outlook_cloud)

The Client Secret value to authenticate with Outlook instance.

#### `Exchange Server` (required if data source is outlook_server)

IP address to connect with Exchange server. Example: `127.0.0.1`

#### `Active Directory Server` (required if data source is outlook_server)

IP address to fetch users from Exchange Active Directory to fetch data. Example: `127.0.0.1`

#### `Exchange server username` (required if data source is outlook_server)

Username to authenticate with Exchange server.

#### `Exchange server password` (required if data source is outlook_server)

Password to authenticate with Exchange server.

#### `Exchange server domain name` (required if data source is outlook_server)

Domain name for Exchange server users such as gmail.com, exchange.local.

#### `Enable SSL`

Whether SSL verification will be enabled. Default value is `False`.

**Note:** This configuration is applicable for `Outlook Server` only.

#### `SSL certificate` (required if ssl is enabled)

Content of SSL certificate. Example certificate:

-----BEGIN CERTIFICATE-----
MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
...
7RhLQyWn2u00L7/9Omw=
-----END CERTIFICATE-----

**Note:** This configuration is applicable for `Outlook Server` only.

### Content Extraction

Refer to [content extraction](https://www.elastic.co/guide/en/enterprise-search/current/connectors-content-extraction.html) in the official docs.

## Documents and syncs

The connector syncs the following objects and entities:
- **Mails**
    - **Inbox Mails**
    - **Sent Mails**
    - **Archive Mails**
    - **Junk Mails**
- **Contacts**
- **Calendar Events**
- **Tasks**
- **Attachments**
    - **Mail Attachments**
    - **Task Attachments**
    - **Calendar Attachments**

*NOTE*:
- Files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/current/sync-rules.html#sync-rules-basic) are identical for all connectors and are available by default.

## Advanced Sync Rules

Advanced sync rules are not available for this connector in the present version.

## Connector Client operations

### End-to-end Testing

**Note:** End-to-end testing is not available in the current version of the connector.

### General 
- Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).
