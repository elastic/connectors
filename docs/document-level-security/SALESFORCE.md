### Setting up the Salesforce connector

See the [Developer guide](../../docs/DEVELOPING.md) for setting up connectors.

## Document level security

Document level security (DLS) enables you to restrict access to documents based on a user'­s permissions. This feature is available by default for the Salesforce connector.
Salesforce connector DLS supports for both standard & custom objects.

Refer to [document level security](https://www.elastic.co/guide/en/enterprise-search/master/dls.html) for more information.

Salesforce allows users to set permissions in different ways i.e. via Profiles, Permission sets and Permission set Groups. 

Refer this tutorial to get more idea on setting the permissions - [link](https://howtovideos.hubs.vidyard.com/watch/B1bQnMFg2VyZq7V6zXQjPg#:~:text=This%20is%20a%20must%20watch,records%20in%20your%20Salesforce%20organization.)

### Set Permissions using Profiles

Follow below steps to set permissions via Profiles:
1. From the setup page, go to `Administration` section => `Users` => `Profiles` and create a new profile
2. Choose `Read Only` or `Standard User` for the Existing Profile dropdown, give a name to the profile and save it. By default, `Read Only` or `Standard User` have the read permission to access all standard objects. There can be some more profiles which do have a read access to standard objects but these are some of them.
3. Now, edit the newly created profile and under `Object Permissions`, assign at least a `Read` access to the standard objects and custom objects you want to ingest into ElasticSearch.

**Note:** If users specify advanced sync rules then they need to assign a `Read` access for that specific object in the profile.

### Set Permissions using Permissions Set

Users can have only one profile but, depending on the Salesforce edition, they can have multiple permission sets. You can assign permission sets to various types of users, regardless of their profiles. Permission sets are used to grant access for a specific job or task.

For example, if the profile does not have read access to any custom object and does not want to update the profile then a permissions set comes into the picture.

We can create a custom Permission Set that will have a read permission to that custom object and assign it to the user. Permission sets can do many more things like setting an app permissions, object permissions, etc.

1. From the setup page, go to `Administration` section => `Users` => `Permission Sets` and create a new permission set.
2. Provide a label to the permission set and select the License for which you want the permission set work.
3. Now, you can set the permissions from the object permissions option. 

### Set Permissions using Permissions Set group

Permission set groups are used to combine the multiple permission sets as per the requirement of the access. 

From the setup page, go to `Administration` section => `Users` => `Permission Set Groups` and create a new permission set group.

Users can add multiple permission sets in a group as well as object permissions can also be set.

### Set Profiles, Permission Set and Permission Set Groups to the User

Go to the `Administration` section => under `Users` section, select `Users` and choose the user to set the permissions to. Now, we can set the Profile, Permission Set and Permission Set Groups created in above steps.

**Note:** Refer to [DLS in Search Applications](https://www.elastic.co/guide/en/enterprise-search/master/dls-e2e-guide.html) to learn how to ingest data from Salesforce with DLS enabled, when building a search application.

#### Additional Configuration

##### `Enable document level security`

Toggle to enable [document level security (DLS)](https://www.elastic.co/guide/en/enterprise-search/master/dls.html). When enabled:
- Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
- Access control syncs will fetch users' access control lists and store them in a separate index.
