<!-- markdownlint-disable -->

<a href="../../package/generated/redis.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.redis`






---

<a href="../../package/generated/redis.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `RedisConnector`
RedisConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`host`</b> (str):  Host 


 - <b>`port`</b> (int):  Port 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`database`</b> (list):  Comma-separated list of databases 
        - Databases are ignored when Advanced Sync Rules are used. 


 - <b>`ssl_enabled`</b> (bool):  SSL/TLS Connection 
        - This option establishes a secure connection to Redis using SSL/TLS encryption. Ensure that your Redis deployment supports SSL/TLS connections. 


 - <b>`mutual_tls_enabled`</b> (bool):  Mutual SSL/TLS Connection 
        - This option establishes a secure connection to Redis using mutual SSL/TLS encryption. Ensure that your Redis deployment supports mutual SSL/TLS connections. 


 - <b>`tls_certfile`</b> (str):  client certificate file for SSL/TLS 
        - Specifies the client certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Redis instance. 


 - <b>`tls_keyfile`</b> (str):  client private key file for SSL/TLS 
        - Specifies the client private key from the Certificate Authority. The value of the key is used to validate the connection in the Redis instance. 

<a href="../../package/generated/redis.py#L45"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    host=None,
    port=None,
    username=None,
    password=None,
    database='*',
    ssl_enabled=False,
    mutual_tls_enabled=False,
    tls_certfile=None,
    tls_keyfile=None,
    **kwargs
)
```









