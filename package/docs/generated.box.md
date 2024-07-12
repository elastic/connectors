<!-- markdownlint-disable -->

<a href="../../package/generated/box.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.box`






---

<a href="../../package/generated/box.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `BoxConnector`
BoxConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`is_enterprise`</b> (str):  Box Account 


 - <b>`client_id`</b> (str):  Client ID 


 - <b>`client_secret`</b> (str):  Client Secret 


 - <b>`refresh_token`</b> (str):  Refresh Token 


 - <b>`enterprise_id`</b> (int):  Enterprise ID 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/box.py#L34"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    is_enterprise='box_free',
    client_id=None,
    client_secret=None,
    refresh_token=None,
    enterprise_id=None,
    concurrent_downloads=15,
    **kwargs
)
```









