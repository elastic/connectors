############################# Sharepoint Online ##############################

## Authentication to Sharepoint Online

Since we use two APIs, we'll need to authenticate two times. That's bad. But not too bad.

What's good is that we can authenticate with same credentials twice and get two tokens for accessing the content we need.

Credentials needed from the user:
- `client_id`
- `client_secret`
- `tenant_id`
- `tenant_name`

Connector can authenticate against Graph API by the following POST request:

```python
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = f"client_id={client_id}&scope=https://graph.microsoft.com/.default&client_secret={client_secret}&grant_type=client_credentials"

    response = request.post(url, headers=headers, data=data).json()

    access_token = response["access_token"]
```

To authenticate in Sharepoint REST API do similar call to a different endpoint:

```python
    url = f"https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
    # GUID in resource is always a constant used to create access token, consider it a magical constant
    data = {
        "grant_type": "client_credentials",
        "resource": f"00000003-0000-0ff1-ce00-000000000000/{tenant_name}.sharepoint.com@{tenant_id}",
        "client_id": f"{client_id}@{tenant_id}",
        "client_secret": client_secret
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(url,data=data, headers=headers).json()

    access_token = response["access_token"]
```

What is also important, that tokens expire, so connector needs to be attentive to which token expired and properly refresh it. Both endpoints used for authentication return a field `expires_in` that specifies how many seconds the token expires in.

## Entities fetched from Sharepoint Online

Sharepoint Online has several ways of accessing the data inside of it:

- Sharepoint REST API (https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/sharepoint-net-server-csom-jsom-and-rest-api-index)
- Graph API (https://learn.microsoft.com/en-us/graph/use-the-api) 

Sharepoint REST API is a legacy way of accessing data in Sharepoint instance. Microsoft discourages from using it unless the data is not accessible through Graph API. So ideally Graph API eventually fully replaces Sharepoint REST API, but right now not every entity is available through Graph API.

Entities that Sharepoint Online connector will fetch:

- Site Collections
- Sites
    - Drives for these sites and files inside these drives
    - Site lists and items inside these lists
    - Site pages

### Site Collections and Sites

See: https://learn.microsoft.com/en-us/sharepoint/sites/sites-and-site-collections-overview

Site Collections and Sites are fetched over Graph API.

Site collection is an abstraction taken from Sharepoint server but seems like slowly being decommissioned.

In essence, site collection is root site on a domain, all other sites belong to it. In practice it will be the site with url `https://{tenant_name}.sharepoint.com`. It seems it's not possible to have multiple site collections for Sharepoint Online.

Sites are contained inside Site Collection - they are created via interface and are always children of the site collection `https://{tenant_name}.sharepoint.com` and have urls `https://{tenant_name}.sharepoint.com/sites/{site_name}`.

Sharepoint Server allows to have subsites but Microsoft discourages against usage of subsites. There's  no option to create subsites in Sharepoint Online by default - it requires some manipulations in admin section `https://{tenant_name}-admin.sharepoint.com`: https://learn.microsoft.com/en-us/sharepoint/manage-site-creation#manage-detailed-site-and-subsite-creation-settings-in-the-classic-sharepoint-admin-center.

Because creation of subsites is discouraged by Microsoft, Sharepoint Online connector does not support subsites.

Group-private sites are not yet supported.

### Drives and Drive Items

See: https://learn.microsoft.com/en-us/graph/api/resources/drive?view=graph-rest-1.0

Drives and Drive Items are fetched over Graph API.

Each Sharepoint Online site by default contains at least one Drive attached to it - API implies, that multiple drives can exist for single site.

Each drive contains a root folder that can contain either folders or files. Graph API makes us fetch folders and items inside of them recursively.

### Lists and List Items

See: https://learn.microsoft.com/en-us/graph/api/resources/list?view=graph-rest-1.0
See: https://support.microsoft.com/en-us/office/introduction-to-lists-0a1c3ace-def0-44af-b225-cfa8d92c52d7

Lists and List Items are fetched over Graph API. List Item Attachments are fetched over Sharepoint REST API.

Each Sharepoint Online site can have multiple lists. It might be hard to understand _what_ a list is. I can describe it as "CSV-file with binary attachments". List contains a collection of entries (list items) that all follow the same flat data structure - keys + values. Each List Item can also have multiple attachments that can be downloaded individualy.

List Item Attachments cannot be fetched with Graph API and are not even available in beta version of Graph API, therefore it's unlikely they'll be accessible except with REST API in any close proximity.

### Site Pages

See: https://learn.microsoft.com/en-us/graph/api/resources/sitepage?view=graph-rest-beta (beta)

Site Pages are fetched over Sharepoint REST API. Possibly later in 2023 they will be available over Graph API, see: https://devblogs.microsoft.com/microsoft365dev/sharepoint-pages-microsoft-graph-api-is-now-available-for-public-preview/ 

Site Pages are self-explanatory - they contain HTML for site pages on Sharepoint Online sites.

Sharepoint Site Pages can have Web Parts that are populated dynamically and their content will not be fetched by default. To actually be able to fetch the content of these Web Parts, user interaction is required, see: https://learn.microsoft.com/en-us/sharepoint/dev/spfx/web-parts/guidance/integrate-web-part-properties-with-sharepoint#specify-web-part-property-value-type

### Is it possible to filter data on the edge?

Yes it's possible but unfortunately it's not useful for initial implementation.

Graph API allows to filter with $filter, see https://learn.microsoft.com/en-us/graph/filter-query-parameter?tabs=http.

Unfortunately not every field can be used for filtering. It's hard to find actual information on _which_ fields can be used for filtering, but it's not possible to use Graph API to filter by metadata - when the document was created, was modified, who created the document, etc. It's possible to filter records with Sharepoint REST API, but REST API has more tight rate limits, thus we avoid using it.

There is also Graph Search API: https://learn.microsoft.com/en-us/graph/search-concept-overview. It cannot be used by connector as it requires Delegated permissions, meaning search works only for specific user, not for the whole deployment.

### What kind of memory profile does the data loaded from the system have?

- Site Collection - super small objects less than a KB
- Site - small objects less than a KB
- Drive - small objects less than a KB
- Drive Item - small objects less than a KB, but require binary download + subextraction and binary content can be as large as file systems allow
- List - small objects less than a KB
- List Item - small objects less than a KB
- List Item Attachments - small objects less than a KB
- Site Page - various size HTML objects that can get large

Overall the responses from Sharepoint are small and don't take too much memory. The biggest memory impact comes from downloading the attachments/drive items and should be handled optimally by the framework.

## What's the setup that external system needs?

### Create a new App Registration in Azure

- Sign in to https://portal.azure.com/ and click on Azure Active Directory.
- Locate App Registrations and Click New Registration.
- Give your app a name - like "Enterprise Search".
- Leave the Redirect URIs blank for now.
- Register the application.
- Find and keep the Application (client) ID and Directory (tenant) ID handy.
- Locate the Client Secret by navigating to Client credentials: Certificates & Secrets.
- Pick a name for your client secret. Select an expiration date. (At this expiration date, you will need to generate a new secret and update your connector configuration.)
- Save the Client Secret value before leaving this screen.
- Set up the permissions the OAuth App will request from the Azure Portal service account.

### Graph API permissions

The following permissions for Graph API are required:

- Sites.Read.All
- Files.Read.All
- Group.Read.All # TODO: confirm with Tim
- User.Read.All # TODO: confirm with Tim

All of the above permissions are Application permissions and require Admin Constent.

### Sharepoint permissions

The following permissions for Sharepoint are required:

- Sites.Read.All

All of the above permissions are Application permissions and require Admin Constent.

Along with adding these permissions, you will need:

1. Authenticate into admin area for your Sharepoint Online deployment: https://{tenant_name}-admin.sharepoint.com/_layouts/15/appinv.aspx
2. Enter the App Id and press "Lookup". App Id is equal to Client Id that can be taken from configuration of your App Registration
3. Enter the App Domain for your site collection: https://{tenant_name}.sharepoint.com
4. Enter the App’s Permission Request XML

```
<AppPermissionRequests AllowAppOnlyPolicy="true">
<AppPermissionRequest Scope="http://sharepoint/content/tenant" Right="FullControl" />
<AppPermissionRequest Scope="http://sharepoint/social/tenant" Right="Read" />
</AppPermissionRequests>
```

5. Press "Create". After it's done the connector will be able to authenticate with Sharepoint REST API

### Additional setup

#### Making Sharepoint Site Pages Web Part content searchable

If you're using Web Parts on Sharepoint Site Pages and want to make their contents searchable, you'll need to follow official documentation: https://devblogs.microsoft.com/microsoft365dev/sharepoint-pages-microsoft-graph-api-is-now-available-for-public-preview/

We recommend setting `isHtmlString` to True for all Web Parts that need to be searchable.

## What's the throttling policy for the system?

Throttling explanation on MS website: https://learn.microsoft.com/en-us/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online

In short, API is throttled per tenant, meaning that horizontal scaling is not really a thing for us. Throttling counter is "Resource Units" that does not strictly map to request number. 1 request can take from 1 to 5 RUs.

Per minute app can consume between 1,200 to 6,000 Resource Units.
Per day app can consume between 1,200,000 to 6,000,000 Resource Units.

Default page size is 100 (not sure if changing it affects the resource consumption). So in the best case we can pull 600,000,000 objects per day, in the worst case we can pull 5 times less = 120,000,000 objects.

These quotas are shared between Graph API and Sharepoint REST API - the latter consumes more resource units.

When requests are throttled, 429 or 503 response is returned. In both cases a "Retry-After" header is included in response and indicates how many seconds we need to wait before performing a new request.

If throttling is not properly handled, the application that disrespects rate limiting might be fully blocked!

Additionally, there are headers that can help understand the throttling during the calls:

```
HTTP/1.1 200 Ok
RateLimit-Limit: 1200
RateLimit-Remaining: 120
RateLimit-Reset: 5
```

The headers above state, that it's possible to use 120 RUs before getting throttled. In 5 seconds the limit will reset and 1200 RUs will be available until next reset.

## What's the permission model for the system?

TODO: write out permission model for Sharepoint Online

## Is it possible to do incremental syncs for the system?

Absolutely! Graph API supports several mechanisms, but the one that's good for us is the [delta sync](https://learn.microsoft.com/en-us/graph/delta-query-overview). Unfortunately, it works only for DriveItems and will work for ListItems in future.

TODO: add more information here

## What is the minimal configuration is required from user to make connector sync data that he needs?

Authentication fields only - Client ID, Secret Value, Tenant ID and Tenant Name. These fields can be used to authenticate and sync everything from the Sharepoint Online instance. More optional fields and advanced filtering rules can be used to limit the amount of data ingested from the Sharepoint Online instance.
