<!-- markdownlint-disable -->

<a href="../../package/generated/notion.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.notion`






---

<a href="../../package/generated/notion.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `NotionConnector`
NotionConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`notion_secret_key`</b> (str):  Notion Secret Key 


 - <b>`databases`</b> (list):  List of Databases 


 - <b>`pages`</b> (list):  List of Pages 


 - <b>`index_comments`</b> (bool):  Enable indexing comments 
        - Enabling this will increase the amount of network calls to the source, and may decrease performance 


 - <b>`concurrent_downloads`</b> (int):  Maximum concurrent downloads 

<a href="../../package/generated/notion.py#L33"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    notion_secret_key=None,
    databases=None,
    pages=None,
    index_comments=False,
    concurrent_downloads=30,
    **kwargs
)
```









