from application.base import BaseDataSource


class MSSQLDataSource(MSSQLDataSource):
    """
    MSSQLDataSource class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        username (str): Username

        password (str): Password

        database (str): Database

        tables (list): Comma-separated list of tables
            - This configurable field is ignored when Advanced Sync Rules are used.

        fetch_size (int): Rows fetched per request

        retry_count (int): Retries per request

        schema (str): Schema

        ssl_enabled (bool): Enable SSL verification

        ssl_ca (str): SSL certificate

        validate_host (bool): Validate host

    """

    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        database=None,
        tables="*",
        fetch_size=None,
        retry_count=None,
        schema=None,
        ssl_enabled=False,
        ssl_ca=None,
        validate_host=False,
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
        self.username = username
        self.password = password
        self.database = database
        self.tables = tables
        self.fetch_size = fetch_size
        self.retry_count = retry_count
        self.schema = schema
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.validate_host = validate_host
