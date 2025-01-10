import os
import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logger(name, log_file, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1)
    handler.suffix = '%Y%m%d'

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

log_file_path = 'logs/log.log'
logger = setup_logger("PertitleEncodingDataGenerator", log_file_path)