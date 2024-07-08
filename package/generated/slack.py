from application.base import BaseDataSource


class SlackDataSource(SlackDataSource):
    """
    SlackDataSource class generated for connecting to the data source.

    Args:

        token (str): Authentication Token
            - The Slack Authentication Token for the slack application you created. See the docs for details.

        fetch_last_n_days (int): Days of message history to fetch
            - How far back in time to request message history from slack. Messages older than this will not be indexed.

        auto_join_channels (bool): Automatically join channels
            - The Slack application bot will only be able to read conversation history from channels it has joined. The default requires it to be manually invited to channels. Enabling this allows it to automatically invite itself into all public channels.

        sync_users (bool): Sync users
            - Whether or not Slack Users should be indexed as documents in Elasticsearch.

    """

    def __init__(
        self,
        token=None,
        fetch_last_n_days=None,
        auto_join_channels=False,
        sync_users=True,
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

        self.token = token
        self.fetch_last_n_days = fetch_last_n_days
        self.auto_join_channels = auto_join_channels
        self.sync_users = sync_users
