# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
#
# This is a generated code. Do not modify directly.
# Run `make generate_connector_package` to update.

from connectors.source import DataSourceConfiguration
from connectors.sources.s3 import S3DataSource
from package.connectors.connector_base import ConnectorBase


class S3Connector(ConnectorBase):
    """
    S3Connector class generated for connecting to the data source.

    Args:

        buckets (list): AWS Buckets
            - AWS Buckets are ignored when Advanced Sync Rules are used.

        aws_access_key_id (str): AWS Access Key Id

        aws_secret_access_key (str): AWS Secret Key

        read_timeout (int): Read timeout

        connect_timeout (int): Connection timeout

        max_attempts (int): Maximum retry attempts

        page_size (int): Maximum size of page

    """

    def __init__(
        self,
        buckets=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        read_timeout=90,
        connect_timeout=90,
        max_attempts=5,
        page_size=100,
    ):

        configuration = S3DataSource.get_default_configuration()

        # Apply the user provided configuration in the class constructor
        args = locals()
        for key in configuration.keys():
            if args.get(key) is not None:
                configuration[key]["value"] = args[key]

        connector_configuration = DataSourceConfiguration(configuration)

        super().__init__(data_provider=S3DataSource(connector_configuration))

        self.buckets = buckets
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_attempts = max_attempts
        self.page_size = page_size
