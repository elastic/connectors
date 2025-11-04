#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
PING_QUERY = """
{
  __schema {
    queryType {
      name
    }
  }
}
"""
RETRIES = 3
RETRY_INTERVAL = 2
BASIC = "basic"
BEARER = "bearer"
CURSOR_PAGINATION = "cursor_pagination"
GET = "get"
NO_PAGINATION = "no_pagination"
POST = "post"

# Regular expression to validate the Base URL
URL_REGEX = "^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)$"
