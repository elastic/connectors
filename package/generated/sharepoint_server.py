from application.base import BaseDataSource


class SharepointServerDataSource(SharepointServerDataSource):
    """
    SharepointServerDataSource class generated for connecting to the data source.

    Args:

        username (str): SharePoint Server username

        password (str): SharePoint Server password

        host_url (str): SharePoint host

        site_collections (list): Comma-separated list of SharePoint site collections to index

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        retry_count (int): Retries per request

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in your SharePoint Server are mirrored in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

        fetch_unique_list_permissions (bool): Fetch unique list permissions
            - Enable this option to fetch unique list permissions. This setting can increase sync time. If this setting is disabled a list will inherit permissions from its parent site.

        fetch_unique_list_item_permissions (bool): Fetch unique list item permissions
            - Enable this option to fetch unique list item permissions. This setting can increase sync time. If this setting is disabled a list item will inherit permissions from its parent site.

    """

    def __init__(
        self,
        username=None,
        password=None,
        host_url=None,
        site_collections=None,
        ssl_enabled=False,
        ssl_ca=None,
        retry_count=None,
        use_text_extraction_service=False,
        use_document_level_security=False,
        fetch_unique_list_permissions=True,
        fetch_unique_list_item_permissions=True,
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

        self.username = username
        self.password = password
        self.host_url = host_url
        self.site_collections = site_collections
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.retry_count = retry_count
        self.use_text_extraction_service = use_text_extraction_service
        self.use_document_level_security = use_document_level_security
        self.fetch_unique_list_permissions = fetch_unique_list_permissions
        self.fetch_unique_list_item_permissions = fetch_unique_list_item_permissions
