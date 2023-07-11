# Google Drive Connector

The [Elastic Google Drive connector](../connectors/sources/google_drive.py) is provided in the Elastic connectors python framework and can be used via [build a connector](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Google Drive** tile from the connectors list or **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Configuring the Google Drive Connector:

You need to configure the Google Drive connector before syncing the data. Fot this you need to create a [service account](https://cloud.google.com/iam/docs/service-account-overview) with appropriate access to Google Drive API.

To get started, log into [Google Cloud Platform](cloud.google.com) and go to the `Console`.

1. **Create a Google Cloud Project.** Give your project a name, change the project ID and click the Create button.

2. **Enable Google APIs.** Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable the Drive API.

3. **Create a Service Account.** In the `APIs & Services` section, click on `Credentials` and click on `Create credentials` to create a service account. Give your service account a name and a service account ID. This is like an email address and will be used to identify your service account in the future. Click `Done` to finish creating the service account.

4. **Create a Key File**. In the Cloud Console, go to IAM and Admin > Service accounts page. Click the email address of the service account that you want to create a key for. Click the `Keys` tab. Click the `Add key` drop-down menu, then select `Create new key`. Select JSON as the Key type and then click `Create`. This will download a JSON file that will contain the service account credentials.

5. **Share Google Drive Folders.** Go to your Google Drive. Right-click the folder or shared drive, choose `Share` and add the email address of the service account you created in step 3. as a viewer to this folder. Note: When you grant a service account access to a specific folder or shared drive in Google Drive, it's important to note that the permissions extend to all the children within that folder or drive. This means that any folders or files contained within the granted folder or drive inherit the same access privileges as the parent.

### Configuration

The following configuration fields need to be provided for setting up the connector:
##### `Google Drive service account JSON`
For Google Drive API, a service account JSON contains credentials and configuration information for your service account. This is the content of a JSON file obtained from step 4. of **Configuring the Google Drive Connector**.

The service account JSON file includes the following information:

- `Client ID`: A unique identifier for the service account.
- `Client Email`: The email address associated with the service account.
- `Private Key`: A private key used to authenticate requests made by the service account.
- `Private Key ID`: An identifier for the private key.
- `Token URI`: The URI where the service account can obtain an access token.
- `Project ID`: The ID of the Google Cloud project associated with the service account.
- `Auth URI`, Token URI, Auth Provider X509 Cert URL: URLs used for authentication and authorization purposes.
- `Client X509 Cert URL`: The URL of the public key certificate associated with the service account.

### Content Extraction

The connector uses the Elastic ingest attachment processor plugin for extracting file contents. The ingest attachment processor extracts files by using the Apache text extraction library Tika. Supported file types eligible for extraction can be found as `TIKA_SUPPORTED_FILETYPES` in [utils.py](../connectors/utils.py) file.

## Documents and syncs

The connector syncs the following objects and entities:
- **Files**
    - Includes metadata such as file name, path, size, content, etc.
- **Folders**
    - Includes metadata such as file name, path, size, etc.
- **Google Suite Documents**
    - Includes metadata such as file name, path, size, content, etc.

**NOTE**:
- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/8.9/sync-rules.html#sync-rules-basic "Basic sync rules") are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).


## E2E Tests

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.
To perform E2E testing for the Google Drive connector, run the following command:
```shell
$ make ftest NAME=google_drive
```

ℹ️ Users can generate the [perf8](https://github.com/elastic/perf8) report using an argument i.e. `PERF8=True`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a Google Drive source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock Google Drive source using the docker image.
