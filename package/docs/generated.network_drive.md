<!-- markdownlint-disable -->

<a href="../../package/generated/network_drive.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.network_drive`






---

<a href="../../package/generated/network_drive.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `NASConnector`
NASConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`username`</b> (str):  Username 


 - <b>`password`</b> (str):  Password 


 - <b>`server_ip`</b> (str):  SMB IP 


 - <b>`server_port`</b> (int):  SMB port 


 - <b>`drive_path`</b> (str):  SMB path 


 - <b>`drive_type`</b> (str):  Drive type 


 - <b>`identity_mappings`</b> (str):  Path of CSV file containing users and groups SID (For Linux Network Drive) 

<a href="../../package/generated/network_drive.py#L36"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    username=None,
    password=None,
    server_ip=None,
    server_port=None,
    drive_path=None,
    drive_type='windows',
    identity_mappings=None,
    **kwargs
)
```









