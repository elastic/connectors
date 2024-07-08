from application.base import BaseDataSource


class BoxDataSource(BoxDataSource):
    """
    BoxDataSource class generated for connecting to the data source.

    Args:

        is_enterprise (str): Box Account

        client_id (str): Client ID

        client_secret (str): Client Secret

        refresh_token (str): Refresh Token

        enterprise_id (int): Enterprise ID

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        is_enterprise="box_free",
        client_id=None,
        client_secret=None,
        refresh_token=None,
        enterprise_id=None,
        concurrent_downloads=None,
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

        self.is_enterprise = is_enterprise
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.enterprise_id = enterprise_id
        self.concurrent_downloads = concurrent_downloads
