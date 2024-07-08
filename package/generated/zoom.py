from application.base import BaseDataSource


class ZoomDataSource(ZoomDataSource):
    """
    ZoomDataSource class generated for connecting to the data source.

    Args:

        account_id (str): Account ID

        client_id (str): Client ID

        client_secret (str): Client secret

        fetch_past_meeting_details (bool): Fetch past meeting details
            - Enable this option to fetch past past meeting details. This setting can increase sync time.

        recording_age (int): Recording Age Limit (Months)
            - How far back in time to request recordings from zoom. Recordings older than this will not be indexed.

        use_text_extraction_service (bool): Use text extraction service
            - Requires a separate deployment of the Elastic Text Extraction Service. Requires that pipeline settings disable text extraction.

    """

    def __init__(
        self,
        account_id=None,
        client_id=None,
        client_secret=None,
        fetch_past_meeting_details=False,
        recording_age=None,
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

        self.account_id = account_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.fetch_past_meeting_details = fetch_past_meeting_details
        self.recording_age = recording_age
        self.use_text_extraction_service = use_text_extraction_service
