<!-- markdownlint-disable -->

<a href="../../package/generated/onedrive.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.onedrive`






---

<a href="../../package/generated/onedrive.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OneDriveConnector`
OneDriveConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`client_id`</b> (str):  Azure application Client ID 


 - <b>`client_secret`</b> (str):  Azure application Client Secret 


 - <b>`tenant_id`</b> (str):  Azure application Tenant ID 


 - <b>`retry_count`</b> (int):  Maximum retries per request 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/onedrive.py#L32"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    client_id=None,
    client_secret=None,
    tenant_id=None,
    retry_count=3,
    concurrent_downloads=15,
    **kwargs
)
```









