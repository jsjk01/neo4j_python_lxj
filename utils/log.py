# @Time    : 2021/4/18 23:07
# @Author  : LXJ
# @FileName: log.py
# @Software: PyCharm

import logging
import sys

# 导入日志配置
from settings import LOG_FMT, LOG_DATEFMT, LOG_FILENAME, LOG_LEVEL


class Logger(object):

    def __init__(self):
        # 1.获取一个logger对象
        self._logger = logging.getLogger()
        # 2.设置format格式
        self.formatter = logging.Formatter(fmt=LOG_FMT, datefmt=LOG_DATEFMT)
        # 3.设置日志输出
        # 3.1 设置文件日志模式
        self._logger.addHandler(self._get_file_handler(LOG_FILENAME))
        # 3.2 设置终端日志模式
        self._logger.addHandler(self._get_console_handler())
        # 3.3 设置日志等级
        self._logger.setLevel(LOG_LEVEL)

    def _get_file_handler(self, filename):
        """
        返回一个日志handler
        :param LOG_FILENAME:
        :return:
        """
        # 1. 获取一个文件日志handler
        file_handler = logging.FileHandler(filename=filename, encoding='utf-8')
        # 2. 设置日志格式
        file_handler.setFormatter(self.formatter)
        # 3. 返回handler
        return file_handler

    def _get_console_handler(self):
        """
        返回一个终端handler
        :return:
        """
        # 1. 获取一个文件日志handler
        console_handler = logging.StreamHandler(sys.stdout)
        # 2. 设置日志格式
        console_handler.setFormatter(self.formatter)
        # 3. 返回handler
        return console_handler

    @property
    def logger(self):
        return self._logger


logger = Logger().logger

if __name__ == '__main__':
    logger.debug('调试信息')
    logger.info('状态信息')
    logger.warning('警告信息')
    logger.error('错误信息')
    logger.critical('严重错误信息')