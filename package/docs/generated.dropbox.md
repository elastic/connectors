<!-- markdownlint-disable -->

<a href="../../package/generated/dropbox.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.dropbox`






---

<a href="../../package/generated/dropbox.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `DropboxConnector`
DropboxConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`path`</b> (str):  Path to fetch files/folders 
        - Path is ignored when Advanced Sync Rules are used. 


 - <b>`app_key`</b> (str):  App Key 


 - <b>`app_secret`</b> (str):  App secret 


 - <b>`refresh_token`</b> (str):  Refresh token 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 


 - <b>`include_inherited_users_and_groups`</b> (bool):  Include groups and inherited users 
        - Include groups and inherited users when indexing permissions. Enabling this configurable field will cause a significant performance degradation. 

<a href="../../package/generated/dropbox.py#L38"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    path=None,
    app_key=None,
    app_secret=None,
    refresh_token=None,
    retry_count=3,
    concurrent_downloads=100,
    include_inherited_users_and_groups=False,
    **kwargs
)
```









