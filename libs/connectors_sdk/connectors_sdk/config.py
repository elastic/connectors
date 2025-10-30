#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
class DataSourceFrameworkConfig:
    """
    The configs that will be exposed to DataSource instances.
    This abstraction prevents DataSource instances from having access to all configuration, while also
    preventing them from requiring substantial changes to access new configs that may be added.
    """

    def __init__(self, max_file_size):
        """
        Should not be called directly. Use the Builder.
        """
        self.max_file_size = max_file_size
