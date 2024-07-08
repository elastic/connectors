from application.base import BaseDataSource


class RedisDataSource(RedisDataSource):
    """
    RedisDataSource class generated for connecting to the data source.

    Args:

        host (str): Host

        port (int): Port

        username (str): Username

        password (str): Password

        database (list): Comma-separated list of databases
            - Databases are ignored when Advanced Sync Rules are used.

        ssl_enabled (bool): SSL/TLS Connection
            - This option establishes a secure connection to Redis using SSL/TLS encryption. Ensure that your Redis deployment supports SSL/TLS connections.

        mutual_tls_enabled (bool): Mutual SSL/TLS Connection
            - This option establishes a secure connection to Redis using mutual SSL/TLS encryption. Ensure that your Redis deployment supports mutual SSL/TLS connections.

        tls_certfile (str): client certificate file for SSL/TLS
            - Specifies the client certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the Redis instance.

        tls_keyfile (str): client private key file for SSL/TLS
            - Specifies the client private key from the Certificate Authority. The value of the key is used to validate the connection in the Redis instance.

    """

    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        database="*",
        ssl_enabled=False,
        mutual_tls_enabled=False,
        tls_certfile=None,
        tls_keyfile=None,
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
        self.ssl_enabled = ssl_enabled
        self.mutual_tls_enabled = mutual_tls_enabled
        self.tls_certfile = tls_certfile
        self.tls_keyfile = tls_keyfile
