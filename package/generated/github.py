from application.base import BaseDataSource


class GitHubDataSource(GitHubDataSource):
    """
    GitHubDataSource class generated for connecting to the data source.

    Args:

        data_source (str): Data source

        host (str): Server URL

        auth_method (str): Authentication method

        token (str): Token

        repo_type (str): Repository Type
            - The Document Level Security feature is not available for the Other Repository Type

        org_name (str): Organization Name

        app_id (int): App ID

        private_key (str): App private key

        repositories (list): List of repositories
            - This configurable field is ignored when Advanced Sync Rules are used.

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Maximum retries per request

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in GitHub are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

    """

    def __init__(
        self,
        data_source="github_server",
        host=None,
        auth_method="personal_access_token",
        token=None,
        repo_type="other",
        org_name=None,
        app_id=None,
        private_key=None,
        repositories=None,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count="3",
        use_text_extraction_service=False,
        use_document_level_security=False,
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
        self.host = host
        self.auth_method = auth_method
        self.token = token
        self.repo_type = repo_type
        self.org_name = org_name
        self.app_id = app_id
        self.private_key = private_key
        self.repositories = repositories
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.use_text_extraction_service = use_text_extraction_service
        self.use_document_level_security = use_document_level_security
