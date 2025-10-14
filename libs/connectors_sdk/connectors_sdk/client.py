#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import abc


class BaseClient(abc.ABC):
    def __init__(self, configuration):
        self.configuration = configuration

    def ping(self):
        """Check the connection to the 3rd party data source."""
        raise NotImplementedError("ping method not implemented")
