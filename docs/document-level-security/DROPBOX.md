### Setting up the Dropbox connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user'­s permissions. This feature is available by default for the Dropbox connector.
Dropbox connector DLS supports each kind of permission such as folder & file permissions and is shared with individuals or groups.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from Dropbox with DLS enabled, when building a search application.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.

##### `Include inherited users and groups`

Toggle to include/exclude groups and inherited users for [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:

- Full sync will include groups and inherited users while indexing permissions. Enabling this configurable field will cause a significant performance degradation.
