<!-- markdownlint-disable -->

<a href="../../package/generated/confluence.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.confluence`






---

<a href="../../package/generated/confluence.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `ConfluenceConnector`
ConfluenceConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`data_source`</b> (str):  Confluence data source 


 - <b>`username`</b> (str):  Confluence Server username 


 - <b>`password`</b> (str):  Confluence Server password 


 - <b>`data_center_username`</b> (str):  Confluence Data Center username 


 - <b>`data_center_password`</b> (str):  Confluence Data Center password 


 - <b>`account_email`</b> (str):  Confluence Cloud account email 


 - <b>`api_token`</b> (str):  Confluence Cloud API token 


 - <b>`confluence_url`</b> (str):  Confluence URL 


 - <b>`spaces`</b> (list):  Confluence space keys 
        - This configurable field is ignored when Advanced Sync Rules are used. 


 - <b>`index_labels`</b> (bool):  Enable indexing labels 
        - Enabling this will increase the amount of network calls to the source, and may decrease performance 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/confluence.py#L52"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    data_source='confluence_server',
    username=None,
    password=None,
    data_center_username=None,
    data_center_password=None,
    account_email=None,
    api_token=None,
    confluence_url=None,
    spaces=None,
    index_labels=False,
    ssl_enabled=False,
    ssl_ca=None,
    retry_count=3,
    concurrent_downloads=50,
    **kwargs
)
```









