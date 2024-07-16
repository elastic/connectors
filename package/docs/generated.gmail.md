<!-- markdownlint-disable -->

<a href="../../package/generated/gmail.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.gmail`






---

<a href="../../package/generated/gmail.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `GMailConnector`
GMailConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`service_account_credentials`</b> (str):  GMail service account JSON 


 - <b>`subject`</b> (str):  Google Workspace admin email 
        - Admin account email address 


 - <b>`customer_id`</b> (str):  Google customer id 
        - Google admin console -> Account -> Settings -> Customer Id 


 - <b>`include_spam_and_trash`</b> (bool):  Include spam and trash emails 
        - Will include spam and trash emails, when set to true. 

<a href="../../package/generated/gmail.py#L33"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    service_account_credentials=None,
    subject=None,
    customer_id=None,
    include_spam_and_trash=False,
    **kwargs
)
```









