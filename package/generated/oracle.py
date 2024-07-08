from application.base import BaseDataSource


class OracleDataSource(OracleDataSource):
    """
    OracleDataSource class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        username (str): Username

        password (str): Password

        connection_source (str): Connection Source
            - Select 'Service Name' option if connecting to a pluggable database

        sid (str): SID

        service_name (str): Service Name

        tables (list): Comma-separated list of tables

        fetch_size (int): Rows fetched per request

        retry_count (int): Retries per request

        oracle_protocol (str): Oracle connection protocol

        oracle_home (str): Path to Oracle Home

        wallet_configuration_path (str): Path to SSL Wallet configuration files

    """

    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        connection_source="sid",
        sid=None,
        service_name=None,
        tables="*",
        fetch_size=None,
        retry_count=None,
        oracle_protocol="TCP",
        oracle_home="",
        wallet_configuration_path=None,
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
        self.connection_source = connection_source
        self.sid = sid
        self.service_name = service_name
        self.tables = tables
        self.fetch_size = fetch_size
        self.retry_count = retry_count
        self.oracle_protocol = oracle_protocol
        self.oracle_home = oracle_home
        self.wallet_configuration_path = wallet_configuration_path
