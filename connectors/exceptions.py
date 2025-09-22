#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Common exceptions for the connectors package.
"""


class DataSourceError(Exception):
    """An exception raised by a data source when something goes wrong."""

    pass