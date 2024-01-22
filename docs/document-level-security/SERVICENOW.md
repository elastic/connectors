### Setting up the ServiceNow connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user's permissions. This feature is available by default for the ServiceNow connector.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

ServiceNow connector supports ACL roles for document level security. For default services, connectors use the below roles to find users who have access to documents.

| Service        | Roles                                                             |
|----------------|-------------------------------------------------------------------|
| User           | admin                                                             |
| Incident       | admin, sn_incident_read, ml_report_user, ml_admin, itil           |
| Requested Item | admin, sn_request_read, asset, atf_test_designer, atf_test_admin  |
| Knowledge      | admin, knowledge, knowledge_manager, knowledge_admin              |
| Change Request | admin, sn_change_read, itil                                       |

For services other than the defaults, the connector iterates over access controls with read operations and finds the respective roles for those services.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from ServiceNow with DLS enabled, when building a search application.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.

#### Limitations

- ServiceNow connector does not support ServiceNow scripted and conditional permissions.
