from celery.utils.log import get_logger


__all__ = ['get_log_obj']


def get_log_obj(name):
    logger = get_logger(name)
    return logger.debug, logger.info, logger.warning, logger.error, logger.critical
