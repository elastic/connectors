# ServiceNow Connector

The [Elastic ServiceNow connector](../connectors/sources/servicenow.py) is provided in the Elastic connectors python framework and can be used as a [connector client](https://www.elastic.co/guide/en/enterprise-search/current/build-connector.html).

## Availability and prerequisites

This connector is available as a **connector client** from the **Python connectors framework**. To use this connector, satisfy all [connector client requirements](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html).

This connector is in **beta** and is subject to change. The design and code is less mature than official GA features and is being provided as-is with no warranties. Beta features are not subject to the support SLA of official GA features.

All services and records the user has access to will be indexed according to the configurations provided.

## Usage

To use this connector as a **connector client**, use the **Customized connector** workflow.

For additional operations, see [Usage](https://www.elastic.co/guide/en/enterprise-search/master/connectors-usage.html).

## Compatibility

ServiceNow "Tokyo", "San Diego" & "Rome" versions are compatible with Elastic connector frameworks.

## Configuration

The following configuration fields are required to set up the connector:

#### `url`

The host url of the ServiceNow instance.

#### `username`

The username of the account for ServiceNow.

#### `password`

The password of the account used for ServiceNow.

#### `services`

Comma-separated list of services to fetch data from ServiceNow. If the value is `*`, the connector will fetch data from the list of basic services provided by ServiceNow: 
- [User](https://docs.servicenow.com/bundle/utah-platform-administration/page/administer/roles/concept/user.html)
- [Incident](https://docs.servicenow.com/bundle/tokyo-it-service-management/page/product/incident-management/concept/c_IncidentManagement.html)
- [Requested Item](https://docs.servicenow.com/bundle/tokyo-servicenow-platform/page/use/service-catalog-requests/task/t_AddNewRequestItems.html)
- [Knowledge](https://docs.servicenow.com/bundle/tokyo-customer-service-management/page/product/customer-service-management/task/t_SearchTheKnowledgeBase.html)
- [Change Request](https://docs.servicenow.com/bundle/tokyo-it-service-management/page/product/change-management/task/t_CreateAChange.html)

Default value is `*`. Examples:

  - `User, Incident, Requested Item, Knowledge, Change Request`
  - `*`

#### `retry_count`

The number of retry attempts after a failed request to ServiceNow. Default value is `3`.

#### `concurrent_downloads`

The number of concurrent downloads for fetching the attachment content. This speeds up the content extraction of attachments. Defaults to `10`.

## Documents and syncs

The connector syncs the following ServiceNow object types: 
- **Records**
- **Attachments**

## Sync rules

- Content of files bigger than 10 MB won't be extracted.
- Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
- Advanced sync rules are not available in the present version. Currently filtering is controlled via ingest pipelines.

## Connector Client operations

### End-to-end Testing

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](https://www.elastic.co/guide/en/enterprise-search/master/build-connector.html#build-connector-testing) for more details.

To perform E2E testing for the ServiceNow connector, run the following command:

```shell
$ make ftest NAME=servicenow
```

ℹ️ Users can generate the performance report using an argument e.g. `PERF8=yes`. Users can also mention the size of the data to be tested for E2E test amongst SMALL, MEDIUM and LARGE by setting up an argument `DATA_SIZE=SMALL`. By Default, it is set to `MEDIUM`.

ℹ️ Users do not need to have a running Elasticsearch instance or a ServiceNow source to run this test. The docker compose file manages the complete setup of the development environment, i.e. both the mock Elastic instance and mock ServiceNow source using the docker image.

## Known issues

There are no known issues for this connector. Refer to [Known issues](https://www.elastic.co/guide/en/enterprise-search/master/connectors-known-issues.html) for a list of known issues for all connectors.

## Troubleshooting

See [Troubleshooting](https://www.elastic.co/guide/en/enterprise-search/master/connectors-troubleshooting.html).

## Security

See [security](https://www.elastic.co/guide/en/enterprise-search/master/connectors-security.html).