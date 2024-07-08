from application.base import BaseDataSource


class NotionDataSource(NotionDataSource):
    """
    NotionDataSource class generated for connecting to the data source.

    Args:

        notion_secret_key (str): Notion Secret Key

        databases (list): List of Databases

        pages (list): List of Pages

        index_comments (bool): Enable indexing comments
            - Enabling this will increase the amount of network calls to the source, and may decrease performance

        concurrent_downloads (int): Maximum concurrent downloads

    """

    def __init__(
        self,
        notion_secret_key=None,
        databases=None,
        pages=None,
        index_comments=False,
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

        self.notion_secret_key = notion_secret_key
        self.databases = databases
        self.pages = pages
        self.index_comments = index_comments
        self.concurrent_downloads = concurrent_downloads
