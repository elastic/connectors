<!-- markdownlint-disable -->

<a href="../../package/generated/github.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.github`






---

<a href="../../package/generated/github.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `GitHubConnector`
GitHubConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`data_source`</b> (str):  Data source 


 - <b>`host`</b> (str):  Server URL 


 - <b>`auth_method`</b> (str):  Authentication method 


 - <b>`token`</b> (str):  Token 


 - <b>`repo_type`</b> (str):  Repository Type 
        - The Document Level Security feature is not available for the Other Repository Type 


 - <b>`org_name`</b> (str):  Organization Name 


 - <b>`app_id`</b> (int):  App ID 


 - <b>`private_key`</b> (str):  App private key 


 - <b>`repositories`</b> (list):  List of repositories 
        - This configurable field is ignored when Advanced Sync Rules are used. 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 


 - <b>`retry_count`</b> (int):  Maximum retries per request 

<a href="../../package/generated/github.py#L48"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    data_source='github_server',
    host=None,
    auth_method='personal_access_token',
    token=None,
    repo_type='other',
    org_name=None,
    app_id=None,
    private_key=None,
    repositories=None,
    ssl_enabled=False,
    ssl_ca=None,
    retry_count=3,
    **kwargs
)
```









