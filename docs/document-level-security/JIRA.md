### Setting up the Jira connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. This feature is available by default for the Jira connector.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from Jira with DLS enabled, when building a search application.

## Additional Pre-requisite

- To access user data on Jira Administration, the account that you created needs to be granted a **Product Access** for `Jira Administration` with `Product Admin` access by an administrator from [Atlassian Admin](http://admin.atlassian.com/).

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). DLS is supported for the Jira connector. When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.