# --*-- utf-8 --*--
import time
import os
import logging
from conf import sysconfig

LOG_PATH = sysconfig.LOG_PATH


class Logger():
    def __init__(self, logname='scaner'):

        curr_day = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        log_path = LOG_PATH
        # if not os.path.exists(log_path):
        #     os.mkdir(log_path)
        log_file = logname+curr_day+'.log'
        # LOG_FILE = "/log/test01.log"

        # handler = logging.handlers.RotatingFileHandler(log_path+log_file, maxBytes=20*1024*1024, backupCount=10)
        handler = logging.FileHandler(os.path.join(log_path, log_file))
        fmt = "%(asctime)s - %(name)s- [%(filename)s:%(lineno)s] - %(levelname)s - %(message)s "
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        self.logger = logging.getLogger(logname)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        #self.logger.setLevel(logging.ERROR)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.exception(msg)

    def debug(self, msg):
        self.logger.debug(msg)

    def warn(self, msg):
        self.logger.warn(msg)
