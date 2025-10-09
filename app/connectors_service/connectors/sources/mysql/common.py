#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
MAX_POOL_SIZE = 10
DEFAULT_FETCH_SIZE = 5000
RETRIES = 3
RETRY_INTERVAL = 2


def format_list(list_):
    return ", ".join(list_)
