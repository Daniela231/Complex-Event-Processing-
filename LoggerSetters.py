import logging
from logging import handlers


def set_logger_handler(logger, filename):
    x=logging.FileHandler("logfiles/"+"ceptest" + str(filename) + ".log", mode='w')
    logger.addHandler(x)
    s=logging.StreamHandler()
    logger.addHandler(s)


def set_logger_rotating_handler(logger, filename):
    x = logging.handlers.RotatingFileHandler("logfiles/"+"ceptest" + str(filename) + ".log", mode='w',
                                             maxBytes=int(2*1024*1024),backupCount=int(300), encoding=None, delay=0)
    logger.addHandler(x)
    s = logging.StreamHandler()
    logger.addHandler(s)
