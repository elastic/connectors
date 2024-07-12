<!-- markdownlint-disable -->

<a href="../../package/generated/servicenow.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.servicenow`






---

<a href="../../package/generated/servicenow.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `ServiceNowConnector`
ServiceNowConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`url`</b> (str):  Service URL 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`services`</b> (list):  Comma-separated list of services 
        - List of services is ignored when Advanced Sync Rules are used. 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/servicenow.py#L35"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    url=None,
    username=None,
    password=None,
    services='*',
    retry_count=3,
    concurrent_downloads=10,
    **kwargs
)
```









