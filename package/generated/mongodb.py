from application.base import BaseDataSource


class MongoDataSource(MongoDataSource):
    """
    MongoDataSource class generated for connecting to the data source.

    Args:

        host (str): Server hostname

        user (str): Username

        password (str): Password

        database (str): Database

        collection (str): Collection

        direct_connection (bool): Direct connection

        ssl_enabled (bool): SSL/TLS Connection
            - This option establishes a secure connection to the MongoDB server using SSL/TLS encryption. Ensure that your MongoDB deployment supports SSL/TLS connections. Enable if MongoDB cluster uses DNS SRV records.

        ssl_ca (str): Certificate Authority (.pem)
            - Specifies the root certificate from the Certificate Authority. The value of the certificate is used to validate the certificate presented by the MongoDB instance.

        tls_insecure (bool): Skip certificate verification
            - This option skips certificate validation for TLS/SSL connections to your MongoDB server. We strongly recommend setting this option to 'disable'.

    """

    def __init__(
        self,
        host=None,
        user=None,
        password=None,
        database=None,
        collection=None,
        direct_connection=False,
        ssl_enabled=False,
        ssl_ca=None,
        tls_insecure=False,
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
        self.user = user
        self.password = password
        self.database = database
        self.collection = collection
        self.direct_connection = direct_connection
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.tls_insecure = tls_insecure
