import logging.handlers
import logging
import os

format_str: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
date_format_str: str = '%m/%d/%Y %H:%M:%S %Z'
logging.basicConfig(format=format_str,
                    datefmt=date_format_str,
                    level=logging.DEBUG)


log_conf_logger = logging.getLogger('logconf')


def get_logger(namespace: str):
    logger = logging.getLogger(namespace)
    return logger


def get_file_logger(namespace: str, log_full_path_file_name_str: str):
    LOGGING_LEVEL = logging.DEBUG
    handler = logging.handlers.RotatingFileHandler(log_full_path_file_name_str, mode='a',
                                                   maxBytes=4000000,
                                                   backupCount=4)
    formatter = logging.Formatter(format_str, date_format_str)
    handler.setFormatter(formatter)
    logger = logging.getLogger(namespace + '-' +
                               log_full_path_file_name_str.replace(os.path.sep, '-').replace(':', ''))
    logger.addHandler(handler)
    logger.setLevel(LOGGING_LEVEL)
    return logger


def shutdown():
    logging.shutdown()
