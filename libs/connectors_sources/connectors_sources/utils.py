#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
import base64


def get_base64_value(content):
    """
    Returns the converted file passed into a base64 encoded value
    Args:
           content (byte): Object content in bytes
    """
    return base64.b64encode(content).decode("utf-8")
