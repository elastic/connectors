<!-- markdownlint-disable -->

<a href="../../package/generated/google_drive.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `generated.google_drive`






---

<a href="../../package/generated/google_drive.py#L14"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

## <kbd>class</kbd> `GoogleDriveConnector`
GoogleDriveConnector class generated for connecting to the data source. 



**Args:**
 


 - <b>`service_account_credentials`</b> (str):  Google Drive service account JSON 
        - This connectors authenticates as a service account to synchronize content from Google Drive. 


 - <b>`use_domain_wide_delegation_for_sync`</b> (bool):  Use domain-wide delegation for data sync 
        - Enable domain-wide delegation to automatically sync content from all shared and personal drives in the Google workspace. This eliminates the need to manually share Google Drive data with your service account, though it may increase sync time. If disabled, only items and folders manually shared with the service account will be synced. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes. 


 - <b>`google_workspace_admin_email_for_data_sync`</b> (str):  Google Workspace admin email 
        - Provide the admin email to be used with domain-wide delegation for data sync. This email enables the connector to utilize the Admin Directory API for listing organization users. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes. 


 - <b>`google_workspace_email_for_shared_drives_sync`</b> (str):  Google Workspace email for syncing shared drives 
        - Provide the Google Workspace user email for discovery and syncing of shared drives. Only the shared drives this user has access to will be synced. 


 - <b>`google_workspace_admin_email`</b> (str):  Google Workspace admin email 
        - In order to use Document Level Security you need to enable Google Workspace domain-wide delegation of authority for your service account. A service account with delegated authority can impersonate admin user with sufficient permissions to fetch all users and their corresponding permissions. Please refer to the connector documentation to ensure domain-wide delegation is correctly configured and has the appropriate scopes. 


 - <b>`max_concurrency`</b> (int):  Maximum concurrent HTTP requests 
        - This setting determines the maximum number of concurrent HTTP requests sent to the Google API to fetch data. Increasing this value can improve data retrieval speed, but it may also place higher demands on system resources and network bandwidth. 

<a href="../../package/generated/google_drive.py#L40"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>method</kbd> `__init__`

```python
__init__(
    service_account_credentials=None,
    use_domain_wide_delegation_for_sync=False,
    google_workspace_admin_email_for_data_sync=None,
    google_workspace_email_for_shared_drives_sync=None,
    google_workspace_admin_email=None,
    max_concurrency=25,
    **kwargs
)
```









