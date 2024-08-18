<!-- markdownlint-disable -->

<a href="../../package/generated/mysql.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.mysql`






---

<a href="../../package/generated/mysql.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `MySqlConnector`
MySqlConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`host`</b> (str):  Host 


 - <b>`port`</b> (int):  Port 


 - <b>`user`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`database`</b> (str):  Database 


 - <b>`tables`</b> (list):  Comma-separated list of tables 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 


 - <b>`fetch_size`</b> (int):  Rows fetched per request 


 - <b>`retry_count`</b> (int):  Retries per request 

<a href="../../package/generated/mysql.py#L42"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    host=None,
    port=None,
    user=None,
    password=None,
    database=None,
    tables='*',
    ssl_enabled=False,
    ssl_ca=None,
    fetch_size=50,
    retry_count=3,
    **kwargs
)
```









