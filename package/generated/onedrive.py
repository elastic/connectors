from application.base import BaseDataSource


class OneDriveDataSource(OneDriveDataSource):
    """
    OneDriveDataSource class generated for connecting to the data source.

    Args:

        client_id (str): Azure application Client ID

        client_secret (str): Azure application Client Secret

        tenant_id (str): Azure application Tenant ID

        retry_count (int): Maximum retries per request

        concurrent_downloads (int): Maximum concurrent downloads

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in OneDrive are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        client_id=None,
        client_secret=None,
        tenant_id=None,
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

        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
        self.use_document_level_security = use_document_level_security
        self.use_text_extraction_service = use_text_extraction_service
