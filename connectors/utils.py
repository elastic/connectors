#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
from datetime import datetime
from connectors.quartz import QuartzCron


def iso_utc(when=None):
    if when is None:
        when = datetime.utcnow()
    return when.isoformat()


def next_run(quartz_definition):
    """Returns the number of seconds before the next run."""
    cron_obj = QuartzCron(quartz_definition, datetime.utcnow())
    when = cron_obj.next_trigger()
    now = datetime.utcnow()
    secs = (when - now).total_seconds()
    if secs < 1.0:
        secs = 0
    return secs
