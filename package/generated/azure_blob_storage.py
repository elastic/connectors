from application.base import BaseDataSource


class AzureBlobStorageDataSource(AzureBlobStorageDataSource):
    """
    AzureBlobStorageDataSource class generated for connecting to the data source.

    Args:

        account_name (str): Azure Blob Storage account name

        account_key (str): Azure Blob Storage account key

        blob_endpoint (str): Azure Blob Storage blob endpoint

        containers (list): Azure Blob Storage containers

        retry_count (int): Retries per request

        concurrent_downloads (int): Maximum concurrent downloads

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        account_name=None,
        account_key=None,
        blob_endpoint=None,
        containers=None,
        retry_count=None,
        concurrent_downloads=None,
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

        self.account_name = account_name
        self.account_key = account_key
        self.blob_endpoint = blob_endpoint
        self.containers = containers
        self.retry_count = retry_count
        self.concurrent_downloads = concurrent_downloads
        self.use_text_extraction_service = use_text_extraction_service
