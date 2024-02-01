### Setting up the Salesforce connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user'­s permissions. This feature is available by default for the Salesforce connector.
Salesforce connector DLS supports for both standard & custom objects.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

Salesforce allows users to set permissions in different ways i.e. via Profiles, Permission sets and Permission set Groups. 

Refer this tutorial to get more idea on setting the permissions - [link](https://howtovideos.hubs.vidyard.com/watch/B1bQnMFg2VyZq7V6zXQjPg#:~:text=This%20is%20a%20must%20watch,records%20in%20your%20Salesforce%20organization.)

### Set Permissions using Profiles

Refer the [official documentation](https://help.salesforce.com/s/articleView?id=sf.admin_userprofiles.htm&type=5) to know how to set permissions via Profiles.

### Set Permissions using Permissions Set

Refer the [official documentation](https://help.salesforce.com/s/articleView?id=sf.perm_sets_overview.htm&language=en_US&type=5) to know how to set permissions via Permissions Sets.

### Set Permissions using Permissions Set group

Refer the [official documentation](https://help.salesforce.com/s/articleView?id=sf.perm_set_groups.htm&type=5) to know how to set permissions via Permissions Set Groups.

### Set Profiles, Permission Set and Permission Set Groups to the User

Go to the `Administration` section => under `Users` section, select `Users` and choose the user to set the permissions to. Now, we can set the Profile, Permission Set and Permission Set Groups created in above steps.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from Salesforce with DLS enabled, when building a search application.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.
