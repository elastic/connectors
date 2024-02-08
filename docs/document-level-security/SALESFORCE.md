### Setting up the Salesforce connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user'­s permissions. This feature is available by default for the Salesforce connector.
Salesforce connector DLS supports for both standard & custom objects.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

Salesforce allows users to set permissions in different ways i.e. via Profiles, Permission sets and Permission set Groups.

For guidance, refer to these [video tutorials](https://howtovideos.hubs.vidyard.com/watch/B1bQnMFg2VyZq7V6zXQjPg#:~:text=This%20is%20a%20must%20watch,records%20in%20your%20Salesforce%20organization) about setting Salesforce permissions.

To ingest any standard or custom objects, users must ensure that at least `Read` permission is granted to that object. This can be granted using any of the following methods for setting permissions.

### Set Permissions using Profiles

Refer to the [official documentation](https://help.salesforce.com/s/articleView?id=sf.admin_userprofiles.htm&type=5) for setting permissions via Profiles.

### Set Permissions using Permissions Set

Refer to the [official documentation](https://help.salesforce.com/s/articleView?id=sf.perm_sets_overview.htm&language=en_US&type=5) for setting permissions via Permissions Sets.

### Set Permissions using Permissions Set group

Refer to the [official documentation](https://help.salesforce.com/s/articleView?id=sf.perm_set_groups.htm&type=5) for setting permissions via Permissions Set Groups.

### Set Profiles, Permission Set and Permission Set Groups to the User

1. Go to `Administration` under the `Users` section.
2. Select `Users` and choose the user to set the permissions to. 
3. Set the `Profile`, `Permission Set`or `Permission Set Groups` created in the earlier steps.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data with DLS enabled, when building a search application.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.
