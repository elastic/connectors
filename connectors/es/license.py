#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from connectors.protocol import JobType


def requires_platinum_license(sync_job, connector, source_klass):
    """Returns whether this scenario requires a Platinum license"""
    return (
        sync_job.job_type == JobType.ACCESS_CONTROL
        and connector.features.document_level_security_enabled()
    ) or source_klass.is_premium()
