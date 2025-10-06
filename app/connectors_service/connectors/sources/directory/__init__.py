#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
<<<<<<< HEAD
from connectors.sources.directory.datasource import (
    DirectoryDataSource as DirectoryDataSource,
)
=======
from .datasource import DirectoryDataSource

__all__ = ["DirectoryDataSource"]
>>>>>>> 9f983d35 (Corrected __init__.py imports by changing to relative imports + add __all__ to prevent ruff from adding a redundant import alias on autoformat + broke out dropbox validator bc I forgot to do it)
