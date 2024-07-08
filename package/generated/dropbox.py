from application.base import BaseDataSource


class DropboxDataSource(DropboxDataSource):
    """
    DropboxDataSource class generated for connecting to the data source.

    Args:

        path (str): Path to fetch files/folders
            - Path is ignored when Advanced Sync Rules are used.

        app_key (str): App Key

        app_secret (str): App secret

        refresh_token (str): Refresh token

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in Dropbox are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

        include_inherited_users_and_groups (bool): Include groups and inherited users
            - Include groups and inherited users when indexing permissions. Enabling this configurable field will cause a significant performance degradation.

    """

    def __init__(
        self,
        path=None,
        app_key=None,
        app_secret=None,
        refresh_token=None,
        retry_count=None,
        concurrent_downloads=None,
        use_text_extraction_service=False,
        use_document_level_security=False,
        include_inherited_users_and_groups=False,
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

        self.path = path
        self.app_key = app_key
        self.app_secret = app_secret
        self.refresh_token = refresh_token
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
        self.use_text_extraction_service = use_text_extraction_service
        self.use_document_level_security = use_document_level_security
        self.include_inherited_users_and_groups = include_inherited_users_and_groups
