### Setting up the Network Drive connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document Level Security (DLS) enables you to restrict access to documents based on a user's permissions.
This feature is available by default for the Network Drive connector.
DLS in the Network drive connector facilitates the syncing of folder & file permissions, including both user and group level permissions.

The native Network Drive connector will offer DLS support for Windows Network Drive only. DLS support for Linux network drive will be available via self-managed connector client.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to search data on a network drive with DLS enabled, when building a search application.

## Additional Pre-requisite

To fetch users and groups in a Windows network drive, account credentials added in the connector configuration should have access to the Powershell of the Windows Server where the network drive is hosted.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.
