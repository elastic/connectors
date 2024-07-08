from application.base import BaseDataSource


class S3DataSource(S3DataSource):
    """
    S3DataSource class generated for connecting to the data source.

    Args:

        buckets (list): AWS Buckets
            - AWS Buckets are ignored when Advanced Sync Rules are used.

        aws_access_key_id (str): AWS Access Key Id

        aws_secret_access_key (str): AWS Secret Key

        read_timeout (int): Read timeout

        connect_timeout (int): Connection timeout

        max_attempts (int): Maximum retry attempts

        page_size (int): Maximum size of page

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        buckets=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        read_timeout=None,
        connect_timeout=None,
        max_attempts=None,
        page_size=None,
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

        self.buckets = buckets
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_attempts = max_attempts
        self.page_size = page_size
        self.use_text_extraction_service = use_text_extraction_service
