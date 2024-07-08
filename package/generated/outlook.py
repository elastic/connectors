from application.base import BaseDataSource


class OutlookDataSource(OutlookDataSource):
    """
    OutlookDataSource class generated for connecting to the data source.

    Args:

        data_source (str): Outlook data source

        tenant_id (str): Tenant ID

        client_id (str): Client ID

        client_secret (str): Client Secret Value

        exchange_server (str): Exchange Server
            - Exchange server's IP address. E.g. 127.0.0.1

        active_directory_server (str): Active Directory Server
            - Active Directory server's IP address. E.g. 127.0.0.1

        username (str): Exchange server username

        password (str): Exchange server password

        domain (str): Exchange server domain name
            - Domain name such as gmail.com, outlook.com

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in Outlook are maintained in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

    """

    def __init__(
        self,
        data_source="outlook_cloud",
        tenant_id=None,
        client_id=None,
        client_secret=None,
        exchange_server=None,
        active_directory_server=None,
        username=None,
        password=None,
        domain=None,
        ssl_enabled=False,
        ssl_ca=None,
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
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.exchange_server = exchange_server
        self.active_directory_server = active_directory_server
        self.username = username
        self.password = password
        self.domain = domain
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.use_text_extraction_service = use_text_extraction_service
        self.use_document_level_security = use_document_level_security
