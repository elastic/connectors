############################# Sharepoint Online ##############################

## How is the data fetched from the system?

Sharepoint Online has several ways of accessing the data inside of it:

- Sharepoint REST API (https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/sharepoint-net-server-csom-jsom-and-rest-api-index)
- MS Graph API (https://learn.microsoft.com/en-us/graph/use-the-api) 

Unfortunately some of the features available in Sharepoint REST API are yet not available in Graph API - namely access to Page content. Thus, we'll have to use both.

## How do we authenticate to the system?

Since we use two APIs, we'll need to authenticate two times. That's bad. But not too bad.

What's good is that we can authenticate with same credentials twice and get two tokens for accessing the content we need.

Credentials needed from the user:
- `client_id`
- `client_secret`
- `tenant_id`
- `tenant_name` #TODO: FIGURE OUT IF TENANT_ID IS SUFFICIENT

To authenticate in Graph API we need [msal](https://pypi.org/project/msal/) package and do the following:

```python
    authority = f"https://login.microsoftonline.com/{tenant_id}"

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority)

    result = app.acquire_token_for_client(scopes=[scope])

    access_token = result["access_token"]
```

To authenticate in Sharepoint REST API we need any http library instead:

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

What is also important, that tokens expire, so our system needs to be attentive to which token expired and properly refresh it.

## What's the setup that external system needs?

### Graph API permissions

TODO: tell which minimal set of permissions is needed to access Graph API for Sharepoint-related data

### Sharepoint permissions

TODO: fun stuff about how to enable Sharepoint to allow for the access that we use for it

## What is the relationship between the data that is loaded from 3rd-party system?

It's a tree structure:

```
Site Collection
    |--- Site
         |--- Drive
              |--- Drive Item
         |--- List
              |--- List Item
         |--- Site Page (Sharepoint API, available only in Beta Graph API)
```

In essence it's possible to parallelise/shard work by sites, drives and lists if needed. 

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

## What kind of memory profile does the data loaded from the system have?

- Site Collection - super small objects less than a KB
- Site - small objects less than a KB
- Drive - small objects less than a KB
- Drive Item - small objects less than a KB, but require binary download + subextraction and binary content can be as large as file systems allow
- List - small objects less than a KB
- List Item - small objects less than a KB
- Site Page - various size HTML objects that can get large

## Is it possible to filter data on the edge?

Oh yes! It's possible and quite flexible - possible to filter by fields and complex expressions.


## Is it possible to do incremental syncs for the system?

Absolutely! Graph API supports several mechanisms, but the one that's good for us is the [delta sync](https://learn.microsoft.com/en-us/graph/delta-query-overview).

It might not be perfect for nested queries - e.g. changing a drive item does not update drive, thus we still need to collect drive items for drive even if the drive was not updated. Some additional investigation is needed.

## What kind of data is useful for us?

## What configuration is required from user to make connector sync data that he needs?

Pretty much only autentication credentials

## What optional configuration can help user with connector?

Sites to sync - not to sync too many sites. Otherwise connector will sync everything, and that's not something that everyone will need.

## What configuration can be validated by the connector so that user will get meaningful error messages?

It's possible to validate that:

1. User provided correct credentials (client_id, client_secret, tenant_id, tenant_name)
2. User provided proper site_collections by iterating over site_collections on the server


Also during connector run-time we can raise meaningful errors that explain that access is denied to certain entities
 
