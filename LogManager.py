import logging


def create(log_name):
    l = logging.getLogger(log_name)
    f = logging.FileHandler(log_name+".log", mode='w')
    l.addHandler(f)
    s = logging.StreamHandler()
    l.addHandler(s)