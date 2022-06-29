import logging

logger = None


def set_logger(log_level=logging.INFO):
    global logger
    if logger is None:
        logger = logging.getLogger("connectors")
        handler = logging.StreamHandler()
        logger.addHandler(handler)
    logger.propagate = False
    logger.setLevel(log_level)
    logger.handlers[0].setLevel(log_level)
    return logger


set_logger()
