# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from elastic_connectors.connectors.source import DataSourceConfiguration
from elastic_connectors.connectors.sources.mongo import MongoDataSource
from elastic_connectors.connector_base import ConnectorBase


class MongoConnector(ConnectorBase):
    """
    MongoConnector class generated for connecting to the data source.

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
        **kwargs
    ):

        configuration = MongoDataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(
            data_provider=MongoDataSource(connector_configuration), **kwargs
        )

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.collection = collection
        self.direct_connection = direct_connection
        self.ssl_enabled = ssl_enabled
        self.ssl_ca = ssl_ca
        self.tls_insecure = tls_insecure
