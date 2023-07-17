# Google Drive Connector

The _Elastic Google Drive connector_ is a connector for [Google Drive](https://www.google.com/drive) data sources.

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

## Usage

To use this connector as a **connector client**, use the **Google Drive** tile from the connectors list or **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Connector authentication prerequisites

Before syncing any data from Google Drive, you need to create a [service account](https://cloud.google.com/iam/docs/service-account-overview) with appropriate access to Google Drive API.

To get started, log into [Google Cloud Platform](cloud.google.com) and go to the `Console`.

1. **Create a Google Cloud Project.** Give your project a name, change the project ID and click the Create button.

2. **Enable Google APIs.** Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable the Drive API.

3. **Create a Service Account.** In the `APIs & Services` section, click on `Credentials` and click on `Create credentials` to create a service account. Give your service account a name and a service account ID. This is like an email address and will be used to identify your service account in the future. Click `Done` to finish creating the service account.

Your service account needs to have access to at least the following scope:
- `https://www.googleapis.com/auth/drive.readonly`

4. **Create a Key File**.
  - In the Cloud Console, go to `IAM and Admin` > `Service accounts` page.
  - Click the email address of the service account that you want to create a key for.
  - Click the `Keys` tab. Click the `Add key` drop-down menu, then select `Create new key`.
  - Select JSON as the Key type and then click `Create`. This will download a JSON file that will contain the service account credentials.

5. **Share Google Drive Folders.** Go to your Google Drive. Right-click the folder or shared drive, choose `Share` and add the email address of the service account you created in step 3. as a viewer to this folder.

  Note: When you grant a service account access to a specific folder or shared drive in Google Drive, it's important to note that the permissions extend to all the children within that folder or drive. This means that any folders or files contained within the granted folder or drive inherit the same access privileges as the parent.


#### Additional authentication prerequisites for document-level security

To access user data on a Google Workspace domain, the service account that you created needs to be granted access by a super administrator for the domain. You can follow [official documentation](https://developers.google.com/cloud-search/docs/guides/delegation) to perform Google Workspace domain-wide delegation of authority.

You need to grant following **OAuth Scopes** to your service account:
- `https://www.googleapis.com/auth/admin.directory.group.readonly`
- `https://www.googleapis.com/auth/admin.directory.user.readonly`

This step allows the connector to access user data and their group memberships in your Google Workspace organization.

## Configuration

The following configuration fields need to be provided for setting up the connector:
##### `Google Drive service account JSON`
The service account credentials generated from Google Cloud Platform (JSON string). Refer to the [Google Cloud documentation](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) for more information.

##### `Enable document level security`
Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). DLS is supported for the Google Drive connector. When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. When enabled, access control syncs will fetch users' access control lists and store them in a separate index.

##### `Google Workspace admin email`
Google Workspace admin email. Required to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). A service account with delegated authority can impersonate an admin user with permissions to access Google Workspace user data and their group memberships. Refer to the [Google Cloud documentation](https://support.google.com/a/answer/162106?hl=en) for more information.

## Deployment using Docker

Follow these instructions to deploy the Google Drive connector using Docker.

#### Step 1: Download sample configuration file

```bash
curl https://raw.githubusercontent.com/elastic/connectors-python/main/config.yml --output ~/connectors-python-config/config.yml
```

Remember to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

#### Step 2: Update the configuration file for your self-managed connector

Update the configuration file with the following settings to match your environment:

- `elasticsearch.host`
- `elasticsearch.password`
- `connector_id`
- `service_type`

Use `google_drive` as the `service_type` value. Don’t forget to uncomment `"google_drive"` in the sources section of the yaml file.

If you’re running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
elasticsearch:
  host: http://host.docker.internal:9200
  username: elastic
  password: <YOUR_PASSWORD>

connector_id: <CONNECTOR_ID_FROM_KIBANA>
service_type: google_drive

sources:
  google_drive: connectors.sources.google_drive:GoogleDriveDataSource
  ...
```

Note that the config file you downloaded might contain more entries, so you will need to manually copy/change the settings that apply to you. Normally you’ll only need to update `elasticsearch.host`, `elasticsearch.password`, `connector_id` and `service_type` to run the connector service.

#### Step 3: Run the Docker image

Run the Docker image with the Connector Service using the following command:

```bash
docker run \
-v ~/connectors-python-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/enterprise-search/elastic-connectors:8.10.0.0-SNAPSHOT \
/app/bin/elastic-ingest \
-c /config/config.yml
```

Refer to [this guide in the Python framework repository](https://github.com/elastic/connectors-python/blob/main/docs/DOCKER.md) for more details.


## Documents and syncs

The connector will fetch all files and folders the service account has access to.

It will attempt to extract the content from Google Suite documents (Google Docs, Google Sheets and Google Slides) and regular files.

**NOTE**:
- Files bigger than 10 MB won't be extracted
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

## Sync rules

[Basic sync rules](https://www.elastic.co/guide/en/enterprise-search/8.9/sync-rules.html#sync-rules-basic "Basic sync rules") are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.

## Document-level security

Document-level security (DLS) enables you to restrict access to documents based on a user’s permissions. This feature is available by default for the Google Drive connector.

Refer to [Document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from Google Drive with DLS enabled, when building a search application.

## Content extraction

See [Content extraction](https://www.elastic.co/guide/en/enterprise-search/master/connectors-content-extraction.html).

## End-to-end testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the Google Drive connector, run the following command:

```shell
make ftest NAME=google_drive
```
For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=google_drive DATA_SIZE=small
```

## Known issues

There are currently no known issues for this connector.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).

## Framework and source

This connector is included in the [Python connectors framework](https://github.com/elastic/connectors-python/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors-python/blob/main/connectors/sources/google_drive.py).
