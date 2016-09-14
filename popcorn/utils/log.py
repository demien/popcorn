import logging
from collections import defaultdict


def get_log_obj(name):
    logger = logging.getLogger(name)
    cache_logger = defaultdict(None)
    if cache_logger.get(name) is not None:
        logger = cache_logger[name]
    else:
        logger = logging.getLogger(name)
        cache_logger[name] = logger

    return logger.debug, logger.info, logger.warning, logger.error, logger.critical
