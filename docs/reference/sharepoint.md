# SharePoint Connector Reference

The [Elastic SharePoint connector](../connectors/sources/sharepoint.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **technical preview** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **build a connector** workflow. See [Connector clients and frameworks](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Configuring the SharePoint Online Connector:
You must configure the SharePoint connector before connecting the SharePoint Online service to Elastic Search. For this you must create an **OAuth App** in the SharePoint Online platform.

To get started, first log in to SharePoint Online and access your administrative dashboard.

**Note:** Ensure you are logged in as the Azure Portal **service account**.
- Sign in to https://portal.azure.com/ and click on **Azure Active Directory**
- Locate **App Registrations** and Click **New Registration**.
- Give your app a name - like "Elastic  Search".

  Leave the Redirect URIs blank for now.
- Register the application
- Retrieve and keep the **Application (client) ID** and **Directory (tenant) ID** handy - we'll need it within Elastic Search.
- Locate the **Client Secret** by navigating to **Client credentials: Certificates & Secrets**
- Pick a name for your client secret (for example, Elastic Search). Select an expiration date. (**Note:** at the end of this expiration date, you will need to generate a new secret and update your connector configuration with it.)
- Save the **Client Secret** value before leaving this screen - we'll need it within Elastic Search.
- We must now set up the permissions the Application will request from the Azure Portal service account. Navigate to **API Permissions** and click **Add Permission**. Add **delegated permissions** until the list resembles the following:
  ```
  User.Read
  ```
- Finally, **Grant admin consent**.

  Use the Grant Admin Consent link from the permissions screen.
- At last also save the tenant name (i.e. Domain name) of Azure platform


## SharePoint Online permissions required to run the connector:

Refer to the following documentation for setting [SharePoint Permissions](https://learn.microsoft.com/en-us/sharepoint/dev/solution-guidance/security-apponly-azureacs)

- To set `DisableCustomAppAuthentication` to false we need to connect to SharePoint using Windows PowerShell and run set-spotenant -DisableCustomAppAuthentication $false
- To assign full permissions to the tenant in SharePoint Online, in your browser, go to the tenant URL. For example, go to `https://<office_365_admin_tenant_URL>/_layouts/15/appinv.aspx` such as `https://contoso-admin.sharepoint.com/_layouts/15/appinv.aspx`. The SharePoint admin center page appears.
    - In the App ID box, enter the application ID that you recorded earlier, and then click Lookup. In the Title box, the name of the application appears.
    - In the App Domain box, type <tenant_name>.onmicrosoft.com
    - In the App's Permission Request XML box, type the following XML string:
        ```
         <AppPermissionRequests AllowAppOnlyPolicy="true">
           <AppPermissionRequest Scope="http://sharepoint/content/tenant" Right="FullControl" />
           <AppPermissionRequest Scope="http://sharepoint/social/tenant" Right="Read" />
         </AppPermissionRequests>
         ```

## Compatibility

Both SharePoint Online and SharePoint server are supported by the connector.

For SharePoint server, below mentioned versions are compatible with Elastic connector frameworks:
- SharePoint 2013
- SharePoint 2016
- SharePoint 2019

## Configuration

The following configuration fields need to be provided for setting up the connector:

#### `data_source`

Determines the SharePoint platform type. `SHAREPOINT_ONLINE` if SharePoint cloud and `SHAREPOINT_SERVER` if SharePoint Server.

#### `username`

The username of the account for SharePoint Server.

**Note:** `username` is not needed for SharePoint Online.

#### `password`

The password of the account to be used for the SharePoint Server.

**Note:** `password` is not needed for SharePoint Online.

#### `client_id`

The client id to authenticate with SharePoint Online.

#### `client_secret`

The secret value to authenticate with SharePoint Online.

#### `tenant`

The tenant name to authenticate with SharePoint Online.

#### `tenant_id`

The tenant id to authenticate with SharePoint Online.

#### `host_url`

The server host url where the SharePoint is hosted. Examples:

- `https://192.158.1.38:8080`
- `https://<tenant_name>.sharepoint.com`

#### `site_collections`

The site collections to fetch sites from SharePoint(allow comma separated collections also). Examples:
- `collection1`
- `collection1, collection2`

#### `ssl_enabled`

Whether SSL verification will be enabled. Default value is `False`.

#### `ssl_ca`

Content of SSL certificate needed for SharePoint Server.

**Note:** Keep this field empty, if `ssl_enabled` is set to `False` (Applicable on SharePoint Server only). Example certificate:


- ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    AlVTMQwwCgYDVQQKEwNJQk0xFjAUBgNVBAsTDURlZmF1bHROb2RlMDExFjAUBgNV
    -----END CERTIFICATE-----
    ```

#### `retry_count`

The number of retry attempts after failed request to the SharePoint. Default value is `3`.

## Documents and syncs
The connector syncs the following SharePoint object types:
- Sites and Subsites
- Lists
- List Items and its attachment content
- Document Libraries and its attachment content(include Web Pages)

## Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.
- Filtering rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## E2E Tests

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the SharePoint connector, run the following command:

```shell
$ make ftest NAME=sharepoint
```

ℹ️ Users can generate the [perf8](https://github.com/elastic/perf8) report using an argument i.e. `PERF8=True`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a SharePoint source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock SharePoint source using the docker image.

ℹ️ The connector uses the Elastic [ingest attachment processor](https://www.elastic.co/guide/en/enterprise-search/current/ingest-pipelines.html) plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.
