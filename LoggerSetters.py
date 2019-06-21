import logging
from logging import handlers


def set_logger_handler(logger, filename):
    x=logging.FileHandler("logfiles/"+"ceptest" + str(filename) + ".log", mode='w')
    logger.addHandler(x)
    s=logging.StreamHandler()
    logger.addHandler(s)


def set_logger_rotating_handler(logger, filename):
    x = logging.handlers.RotatingFileHandler("logfiles/"+"ceptest" + str(filename) + ".log", mode='w', maxBytes=10)
    logger.addHandler(x)
    s = logging.StreamHandler()
    logger.addHandler(s)
