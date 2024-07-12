<!-- markdownlint-disable -->

<a href="../../package/generated/postgresql.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.postgresql`






---

<a href="../../package/generated/postgresql.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `PostgreSQLConnector`
PostgreSQLConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`host`</b> (str):  Host 


 - <b>`port`</b> (int):  Port 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`database`</b> (str):  Database 


 - <b>`schema`</b> (str):  Schema 


 - <b>`tables`</b> (list):  Comma-separated list of tables 
        - This configurable field is ignored when Advanced Sync Rules are used. 


 - <b>`fetch_size`</b> (int):  Rows fetched per request 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL verification 


 - <b>`ssl_ca`</b> (str):  SSL certificate 

<a href="../../package/generated/postgresql.py#L45"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    host=None,
    port=None,
    username=None,
    password=None,
    database=None,
    schema=None,
    tables='*',
    fetch_size=50,
    retry_count=3,
    ssl_enabled=False,
    ssl_ca=None,
    **kwargs
)
```









