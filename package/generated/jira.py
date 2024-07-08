from application.base import BaseDataSource


class JiraDataSource(JiraDataSource):
    """
    JiraDataSource class generated for connecting to the data source.

    Args:

        data_source (str): Jira data source

        username (str): Jira Server username

        password (str): Jira Server password

        data_center_username (str): Jira Data Center username

        data_center_password (str): Jira Data Center password

        account_email (str): Jira Cloud email address
            - Email address associated with Jira Cloud account. E.g. jane.doe@gmail.com

        api_token (str): Jira Cloud API token

        jira_url (str): Jira host url

        projects (list): Jira project keys
            - This configurable field is ignored when Advanced Sync Rules are used.

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Retries for failed requests

        concurrent_downloads (int): Maximum concurrent downloads

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in Jira are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents. Only 1000 users can be fetched for Jira Data Center.

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        data_source="jira_cloud",
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
        retry_count=None,
        concurrent_downloads=None,
        use_document_level_security=False,
        use_text_extraction_service=False,
    ):
        configuration = self.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args[key] is not None:
                configuration[key]["value"] = args[key]

        # Check if all fields marked as 'required' in config are present with values, if not raise an exception
        for key, value in configuration.items():
            if value["value"] is None and value.get("required", True):
                raise ValueError(f"Missing required configuration field: {key}")

        super().__init__(configuration)

        self.data_source = data_source
        self.username = username
        self.password = password
        self.data_center_username = data_center_username
        self.data_center_password = data_center_password
        self.account_email = account_email
        self.api_token = api_token
        self.jira_url = jira_url
        self.projects = projects
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
        self.use_document_level_security = use_document_level_security
        self.use_text_extraction_service = use_text_extraction_service
