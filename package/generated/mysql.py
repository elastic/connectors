from application.base import BaseDataSource


class MySqlDataSource(MySqlDataSource):
    """
    MySqlDataSource class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        user (str): Username

        password (str): Password

        database (str): Database

        tables (list): Comma-separated list of tables

        ssl_enabled (bool): Enable SSL

        ssl_ca (str): SSL certificate

        fetch_size (int): Rows fetched per request

        retry_count (int): Retries per request

    """

    def __init__(
        self,
        host=None,
        port=None,
        user=None,
        password=None,
        database=None,
        tables="*",
        ssl_enabled=False,
        ssl_ca=None,
        fetch_size=None,
        retry_count=None,
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

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.tables = tables
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.fetch_size = fetch_size
        self.retry_count = retry_count
