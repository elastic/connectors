<!-- markdownlint-disable -->

<a href="../../package/generated/outlook.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.outlook`






---

<a href="../../package/generated/outlook.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OutlookConnector`
OutlookConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`data_source`</b> (str):  Outlook data source 


 - <b>`tenant_id`</b> (str):  Tenant ID 


 - <b>`client_id`</b> (str):  Client ID 


 - <b>`client_secret`</b> (str):  Client Secret Value 


 - <b>`exchange_server`</b> (str):  Exchange Server 
        - Exchange server's IP address. E.g. 127.0.0.1 


 - <b>`active_directory_server`</b> (str):  Active Directory Server 
        - Active Directory server's IP address. E.g. 127.0.0.1 


 - <b>`username`</b> (str):  Exchange server username 


 - <b>`password`</b> (str):  Exchange server password 


 - <b>`domain`</b> (str):  Exchange server domain name 
        - Domain name such as gmail.com, outlook.com 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 

<a href="../../package/generated/outlook.py#L47"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    data_source='outlook_cloud',
    tenant_id=None,
    client_id=None,
    client_secret=None,
    exchange_server=None,
    active_directory_server=None,
    username=None,
    password=None,
    domain=None,
    ssl_enabled=False,
    ssl_ca=None,
    **kwargs
)
```









