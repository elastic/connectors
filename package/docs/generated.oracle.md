<!-- markdownlint-disable -->

<a href="../../package/generated/oracle.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.oracle`






---

<a href="../../package/generated/oracle.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `OracleConnector`
OracleConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`host`</b> (str):  Host 


 - <b>`port`</b> (int):  Port 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`connection_source`</b> (str):  Connection Source 
        - Select 'Service Name' option if connecting to a pluggable database 


 - <b>`sid`</b> (str):  SID 


 - <b>`service_name`</b> (str):  Service Name 


 - <b>`tables`</b> (list):  Comma-separated list of tables 


 - <b>`fetch_size`</b> (int):  Rows fetched per request 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`oracle_protocol`</b> (str):  Oracle connection protocol 


 - <b>`oracle_home`</b> (str):  Path to Oracle Home 


 - <b>`wallet_configuration_path`</b> (str):  Path to SSL Wallet configuration files 

<a href="../../package/generated/oracle.py#L49"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    host=None,
    port=None,
    username=None,
    password=None,
    connection_source='sid',
    sid=None,
    service_name=None,
    tables='*',
    fetch_size=50,
    retry_count=3,
    oracle_protocol='TCP',
    oracle_home='',
    wallet_configuration_path='',
    **kwargs
)
```









