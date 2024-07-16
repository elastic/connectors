<!-- markdownlint-disable -->

<a href="../../package/generated/sharepoint_server.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.sharepoint_server`






---

<a href="../../package/generated/sharepoint_server.py#L16"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `SharepointServerConnector`
SharepointServerConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`username`</b> (str):  SharePoint Server username 


 - <b>`password`</b> (str):  SharePoint Server password 


 - <b>`host_url`</b> (str):  SharePoint host 


 - <b>`site_collections`</b> (list):  Comma-separated list of SharePoint site collections to index 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`fetch_unique_list_permissions`</b> (bool):  Fetch unique list permissions 
        - Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site. 


 - <b>`fetch_unique_list_item_permissions`</b> (bool):  Fetch unique list item permissions 
        - Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site. 

<a href="../../package/generated/sharepoint_server.py#L44"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    username=None,
    password=None,
    host_url=None,
    site_collections=None,
    ssl_enabled=False,
    ssl_ca=None,
    retry_count=3,
    fetch_unique_list_permissions=True,
    fetch_unique_list_item_permissions=True,
    **kwargs
)
```









