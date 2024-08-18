<!-- markdownlint-disable -->

<a href="../../package/generated/sharepoint_online.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.sharepoint_online`






---

<a href="../../package/generated/sharepoint_online.py#L16"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `SharepointOnlineConnector`
SharepointOnlineConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`tenant_id`</b> (str):  Tenant ID 


 - <b>`tenant_name`</b> (str):  Tenant name 


 - <b>`client_id`</b> (str):  Client ID 


 - <b>`secret_value`</b> (str):  Secret value 


 - <b>`site_collections`</b> (list):  Comma-separated list of sites 
        - A comma-separated list of sites to ingest data from. If enumerating all sites, use * to include all available sites, or specify a list of site names. Otherwise, specify a list of site paths. 


 - <b>`enumerate_all_sites`</b> (bool):  Enumerate all sites? 
        - If enabled, sites will be fetched in bulk, then filtered down to the configured list of sites. This is efficient when syncing many sites. If disabled, each configured site will be fetched with an individual request. This is efficient when syncing fewer sites. 


 - <b>`fetch_subsites`</b> (bool):  Fetch sub-sites of configured sites? 
        - Whether subsites of the configured site(s) should be automatically fetched. 


 - <b>`fetch_drive_item_permissions`</b> (bool):  Fetch drive item permissions 
        - Enable this option to fetch drive item specific permissions. This setting can increase sync time. 


 - <b>`fetch_unique_page_permissions`</b> (bool):  Fetch unique page permissions 
        - Enable this option to fetch unique page permissions. This setting can increase sync time. If this setting is disabled a page will inherit permissions from its parent site. 


 - <b>`fetch_unique_list_permissions`</b> (bool):  Fetch unique list permissions 
        - Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site. 


 - <b>`fetch_unique_list_item_permissions`</b> (bool):  Fetch unique list item permissions 
        - Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site. 

<a href="../../package/generated/sharepoint_online.py#L53"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    tenant_id=None,
    tenant_name=None,
    client_id=None,
    secret_value=None,
    site_collections='*',
    enumerate_all_sites=True,
    fetch_subsites=True,
    fetch_drive_item_permissions=True,
    fetch_unique_page_permissions=True,
    fetch_unique_list_permissions=True,
    fetch_unique_list_item_permissions=True,
    **kwargs
)
```









