# -*- coding: UTF-8 -*-
import logging
import sys
import datetime

rootLogger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s [%(process)d-%(thread)d] - %(filename)s[line:%(lineno)d] - %(levelname)s: '
                              '%(message)s')
if not rootLogger.handlers:
    root_handler = logging.StreamHandler(sys.stdout)
    root_handler.setFormatter(formatter)
    root_handler.setLevel(logging.INFO)
    rootLogger.addHandler(root_handler)

class log:
    def __init__(self):
        pass

    # ERROR
    # WARNING
    # INFO
    # DEBUG
    @staticmethod
    def get_logger(category, configs=None):
        # 默认设置
        file_path = None
        file_level = logging.INFO
        # 读取配置
        if configs is not None:
            file_path = configs['file_path']
            file_level = configs['file_level']
            root_handler.setLevel(configs['console_level'])
        logger = logging.getLogger(category)
        if not logger.handlers:
            # 设置全局日志级别
            logger.setLevel(logging.DEBUG)
            # 创建一个handler，用于写入日志文件
            if file_path != "" and file_path is not None:
                nowTime = datetime.datetime.now().strftime("%Y_%m_%d")
                file_Path = "%s%s%s%s" % (file_path, "logger_", nowTime, ".log")
                file_handler = logging.FileHandler(file_Path, mode='a')
                file_handler.setLevel(file_level)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)

        return logger
