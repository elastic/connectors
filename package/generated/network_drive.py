from application.base import BaseDataSource


class NASDataSource(NASDataSource):
    """
    NASDataSource class generated for connecting to the data source.

    Args:

        username (str): Username

        password (str): Password

        server_ip (str): SMB IP

        server_port (int): SMB port

        drive_path (str): SMB path

        use_document_level_security (bool): Enable document level security
            - Document level security ensures identities and permissions set in your network drive are mirrored in Elasticsearch. This enables you to restrict and personalize read-access users and groups have to documents in this index. Access control syncs ensure this metadata is kept up to date in your Elasticsearch documents.

        drive_type (str): Drive type

        identity_mappings (str): Path of CSV file containing users and groups SID (For Linux Network Drive)

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        username=None,
        password=None,
        server_ip=None,
        server_port=None,
        drive_path=None,
        use_document_level_security=False,
        drive_type="windows",
        identity_mappings=None,
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

        self.username = username
        self.password = password
        self.server_ip = server_ip
        self.server_port = server_port
        self.drive_path = drive_path
        self.use_document_level_security = use_document_level_security
        self.drive_type = drive_type
        self.identity_mappings = identity_mappings
        self.use_text_extraction_service = use_text_extraction_service
