<!-- markdownlint-disable -->

<a href="../../package/generated/azure_blob_storage.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.azure_blob_storage`






---

<a href="../../package/generated/azure_blob_storage.py#L16"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `AzureBlobStorageConnector`
AzureBlobStorageConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`account_name`</b> (str):  Azure Blob Storage account name 


 - <b>`account_key`</b> (str):  Azure Blob Storage account key 


 - <b>`blob_endpoint`</b> (str):  Azure Blob Storage blob endpoint 


 - <b>`containers`</b> (list):  Azure Blob Storage containers 


 - <b>`retry_count`</b> (int):  Retries per request 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/azure_blob_storage.py#L36"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    account_name=None,
    account_key=None,
    blob_endpoint=None,
    containers=None,
    retry_count=3,
    concurrent_downloads=100,
    **kwargs
)
```









