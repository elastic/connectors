#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Database utilities for connectors."""

from typing import List, Tuple, Union


class CursorSync:
    """A synchronous cursor interface for database operations."""

    def __enter__(self):
        """Make a database connection and return it"""
        return self

    def __init__(self, query_object, *args, **kwargs) -> None:
        """Setup cursor"""
        self.query = kwargs.get("statement", "")
        self.query_object = query_object

    def keys(self) -> List[str]:
        """Return Columns of table

        Returns:
            list: List of columns
        """
        return []

    def fetchmany(self, size: int) -> List[Union[Tuple[int, str], Tuple[str], Tuple[int]]]:
        """This method returns response of fetchmany

        Args:
            size (int): Number of rows

        Returns:
            list: List of rows
        """
        return []

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager"""
        pass