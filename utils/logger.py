# --*-- utf-8 --*--
import time
import os
import logging
from logging import handlers


class Logger():
    def __init__(self, logname='scaner'):

        log_path = os.path.join(os.getcwd(), 'logs/')

        handler = logging.handlers.RotatingFileHandler(log_path+logname, maxBytes=20*1024*1024, backupCount=10)
        fmt = "%(asctime)s - %(name)s- [%(filename)s:%(lineno)s] - %(levelname)s - %(message)s "
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(logname)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        #self.logger.setLevel(logging.ERROR)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.exception(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warn(self, msg):
        self.logger.warn(msg)
