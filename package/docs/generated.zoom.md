<!-- markdownlint-disable -->

<a href="../../package/generated/zoom.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.zoom`






---

<a href="../../package/generated/zoom.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `ZoomConnector`
ZoomConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`account_id`</b> (str):  Account ID 


 - <b>`client_id`</b> (str):  Client ID 


 - <b>`client_secret`</b> (str):  Client secret 


 - <b>`fetch_past_meeting_details`</b> (bool):  Fetch past meeting details 
        - Enable this option to fetch past past meeting details. This setting can increase sync time. 


 - <b>`recording_age`</b> (int):  Recording Age Limit (Months) 
        - How far back in time to request recordings from zoom. Recordings older than this will not be indexed. 

<a href="../../package/generated/zoom.py#L34"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    account_id=None,
    client_id=None,
    client_secret=None,
    fetch_past_meeting_details=False,
    recording_age=None,
    **kwargs
)
```









