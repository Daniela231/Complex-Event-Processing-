import logging
import logging.handlers


def set_logger_handler(logger, filename):
    x=logging.FileHandler("logfiles/"+"ceptest" + str(filename) + ".log", mode='w')
    logger.addHandler(x)
    s=logging.StreamHandler()
    logger.addHandler(s)


"""
def set_logger_rotatinghandler(logger, filename):
    x = logging.BaseRotatingHandler("ceptest" + str(filename) + ".log", mode='w')
    logger.addHandler(x)
    s = logging.StreamHandler()
    logger.addHandler(s)
"""