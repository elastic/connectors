#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import abc

from connectors_sdk.retry import retryable


class BaseClient(abc.ABC):
    def __init__(self, configuration):
        self.configuration = configuration

    @abc.abstractmethod
    def ping(self):
        """Check the connection to the 3rd party data source."""
        raise NotImplementedError("ping method not implemented")

    @abc.abstractmethod
    def close(self):
        """Close the client connection."""
        raise NotImplementedError("close method not implemented")

    @retryable
    async def api_call(self, *args, **kwargs):
        """Make an API call to the 3rd party data source."""
        self._api_call(*args, **kwargs)

    @abc.abstractmethod
    def _api_call(self, *args, **kwargs):
        raise NotImplementedError("_api_call method not implemented")

    @abc.abstractmethod
    def api_call_paginated(self, *args, **kwargs):
        """Make a paginated API call to the 3rd party data source."""
        raise NotImplementedError("api_call_paginated method not implemented")

