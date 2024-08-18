<!-- markdownlint-disable -->

<a href="../../package/generated/jira.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.jira`






---

<a href="../../package/generated/jira.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `JiraConnector`
JiraConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`data_source`</b> (str):  Jira data source 


 - <b>`username`</b> (str):  Jira Server username 


 - <b>`password`</b> (str):  Jira Server password 


 - <b>`data_center_username`</b> (str):  Jira Data Center username 


 - <b>`data_center_password`</b> (str):  Jira Data Center password 


 - <b>`account_email`</b> (str):  Jira Cloud email address 
        - Email address associated with Jira Cloud account. E.g. jane.doe@gmail.com 


 - <b>`api_token`</b> (str):  Jira Cloud API token 


 - <b>`jira_url`</b> (str):  Jira host url 


 - <b>`projects`</b> (list):  Jira project keys 
        - This configurable field is ignored when Advanced Sync Rules are used. 


 - <b>`ssl_enabled`</b> (bool):  Enable SSL 


 - <b>`ssl_ca`</b> (str):  SSL certificate 


 - <b>`retry_count`</b> (int):  Retries for failed requests 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/jira.py#L50"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    data_source='jira_cloud',
    username=None,
    password=None,
    data_center_username=None,
    data_center_password=None,
    account_email=None,
    api_token=None,
    jira_url=None,
    projects=None,
    ssl_enabled=False,
    ssl_ca=None,
    retry_count=3,
    concurrent_downloads=100,
    **kwargs
)
```









