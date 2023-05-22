#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""
Logger -- sets the logging and provides a `logger` global object.
"""
import logging
from datetime import datetime

import ecs_logging

from connectors import __version__

logger = None


def _formatter(prefix):
    return logging.Formatter(
        fmt="[" + prefix + "][%(asctime)s][%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


class CustomAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None, filebeat=False):
        super().__init__(logger, extra=extra)
        self.filebeat = filebeat

    def process(self, msg, kwargs):
        sync_job = kwargs.pop("sync_job", None)
        connector = kwargs.pop("connector", None)
        if self.filebeat:
            if "extra" not in kwargs:
                kwargs["extra"] = {}
            kwargs["extra"].update(
                {
                    "service.type": "connectors-python",
                    "service.version": __version__,
                    "labels.index_date": datetime.now().strftime("%Y.%m.%d"),
                }
            )
            if sync_job:
                kwargs["extra"].update(
                    {
                        "labels.sync_job_id": sync_job.id,
                        "labels.connector_id": sync_job.connector_id,
                        "labels.index_name": sync_job.index_name,
                    }
                )
            elif connector:
                kwargs["extra"].update(
                    {
                        "labels.connector_id": connector.id,
                        "labels.index_name": connector.index_name,
                    }
                )
        else:
            if sync_job:
                msg = "[Sync Job id: %s, connector id: %s, index name: %s] %s" % (
                    sync_job.id,
                    sync_job.connector_id,
                    sync_job.index_name,
                    msg,
                )
            elif connector:
                msg = "[Connector id: %s, index name: %s] %s" % (
                    connector.id,
                    connector.index_name,
                    msg,
                )
        return msg, kwargs


def set_logger(log_level=logging.INFO, filebeat=False):
    global logger
    if filebeat:
        formatter = ecs_logging.StdlibFormatter()
    else:
        formatter = _formatter("FMWK")

    if logger is None:
        logger_ = logging.getLogger("connectors")
        handler = logging.StreamHandler()
        logger_.addHandler(handler)
        logger = CustomAdapter(logger_, filebeat=filebeat)

    logger.logger.propagate = False
    logger.logger.setLevel(log_level)
    logger.logger.handlers[0].setLevel(log_level)
    logger.logger.handlers[0].setFormatter(formatter)
    logger.filebeat = filebeat
    return logger


def set_extra_logger(logger, log_level=logging.INFO, prefix="BYOC", filebeat=False):
    if isinstance(logger, str):
        logger = logging.getLogger(logger)
    handler = logging.StreamHandler()
    if filebeat:
        handler.setFormatter(ecs_logging.StdlibFormatter())
    else:
        formatter = _formatter(prefix)
        handler.setFormatter(formatter)
    handler.setLevel(log_level)
    logger.addHandler(handler)
    logger.setLevel(log_level)


set_logger()
