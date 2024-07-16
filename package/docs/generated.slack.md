<!-- markdownlint-disable -->

<a href="../../package/generated/slack.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.slack`






---

<a href="../../package/generated/slack.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `SlackConnector`
SlackConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`token`</b> (str):  Authentication Token 
        - The Slack Authentication Token for the slack application you created. See the docs for details. 


 - <b>`fetch_last_n_days`</b> (int):  Days of message history to fetch 
        - How far back in time to request message history from slack. Messages older than this will not be indexed. 


 - <b>`auto_join_channels`</b> (bool):  Automatically join channels 
        - The Slack application bot will only be able to read conversation history from channels it has joined. The default requires it to be manually invited to channels. Enabling this allows it to automatically invite itself into all public channels. 


 - <b>`sync_users`</b> (bool):  Sync users 
        - Whether or not Slack Users should be indexed as documents in Elasticsearch. 

<a href="../../package/generated/slack.py#L34"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    token=None,
    fetch_last_n_days=None,
    auto_join_channels=False,
    sync_users=True,
    **kwargs
)
```









