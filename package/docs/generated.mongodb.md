<!-- markdownlint-disable -->

<a href="../../package/generated/mongodb.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.mongodb`






---

<a href="../../package/generated/mongodb.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `MongoConnector`
MongoConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`host`</b> (str):  Server hostname 


 - <b>`user`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`database`</b> (str):  Database 


 - <b>`collection`</b> (str):  Collection 


 - <b>`direct_connection`</b> (bool):  Direct connection 


 - <b>`ssl_enabled`</b> (bool):  SSL/TLS Connection 
        - This option establishes a secure connection to the MongoDB server using SSL/TLS encryption. Ensure that your MongoDB deployment supports SSL/TLS connections. Enable if MongoDB cluster uses DNS SRV records. 


 - <b>`ssl_ca`</b> (str):  Certificate Authority (.pem) 
        - Specifies the root certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the MongoDB instance. 


 - <b>`tls_insecure`</b> (bool):  Skip certificate verification 
        - This option skips certificate validation for TLS/SSL connections to your MongoDB server. We strongly recommend setting this option to 'disable'. 

<a href="../../package/generated/mongodb.py#L43"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    host=None,
    user=None,
    password=None,
    database=None,
    collection=None,
    direct_connection=False,
    ssl_enabled=False,
    ssl_ca=None,
    tls_insecure=False,
    **kwargs
)
```









