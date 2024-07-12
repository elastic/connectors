<!-- markdownlint-disable -->

<a href="../../package/generated/s3.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.s3`






---

<a href="../../package/generated/s3.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `S3Connector`
S3Connector class generated for connecting to the data source. 



**Args:**
 


 - <b>`buckets`</b> (list):  AWS Buckets 
        - AWS Buckets are ignored when Advanced Sync Rules are used. 


 - <b>`aws_access_key_id`</b> (str):  AWS Access Key Id 


 - <b>`aws_secret_access_key`</b> (str):  AWS Secret Key 


 - <b>`read_timeout`</b> (int):  Read timeout 


 - <b>`connect_timeout`</b> (int):  Connection timeout 


 - <b>`max_attempts`</b> (int):  Maximum retry attempts 


 - <b>`page_size`</b> (int):  Maximum size of page 

<a href="../../package/generated/s3.py#L37"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    buckets=None,
    aws_access_key_id=None,
    aws_secret_access_key=None,
    read_timeout=90,
    connect_timeout=90,
    max_attempts=5,
    page_size=100,
    **kwargs
)
```









