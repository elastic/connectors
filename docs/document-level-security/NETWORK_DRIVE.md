### Setting up the Network Drive connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document Level Security (DLS) enables you to restrict access to documents based on a user's permissions.
This feature is available by default for the Network Drive connector.
Network drive DLS supports folder & file permissions that are shared with individuals or groups.

The native Netowrk Drive connector will offer DLS support for Windows Network Drive only.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to search data from Network Drive with DLS enabled, when building a search application.

## Additional Pre-requisite

In order to fetch users and groups in a Windows Network Drive, the account credentials added in the connector configuration should have access to the Powershell of the Windows Server where the Network Drive is hosted.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.
