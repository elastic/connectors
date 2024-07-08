from application.base import BaseDataSource


class GoogleCloudStorageDataSource(GoogleCloudStorageDataSource):
    """
    GoogleCloudStorageDataSource class generated for connecting to the data source.

    Args:

        buckets (list): Google Cloud Storage buckets

        service_account_credentials (str): Google Cloud service account JSON

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        buckets=None,
        service_account_credentials=None,
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
        self.service_account_credentials = service_account_credentials
        self.use_text_extraction_service = use_text_extraction_service
