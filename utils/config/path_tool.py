# -*- coding: UTF-8 -*-

import os
import platform
import path_tool


class path:
    def __init__(self):
        pass

    @staticmethod
    def convert(param_path):
        system = platform.system()
        # 判断是否为Windows
        if system == 'Windows':
            param_path = param_path.replace("${", "%")
            param_path = param_path.replace("}", "%")

        default_base_dir = path_tool.path.parent(__file__, 3)
        # 项目路径【默认为当程序路径】
        if os.environ.get("SIM_PLATFORM_HOME") is None:
            os.environ['SIM_PLATFORM_HOME'] = default_base_dir

        output = os.popen("echo " + param_path)
        return output.readline().replace("\n", "")

    @staticmethod
    def parent(param_path, level=1):
        parent_path = param_path
        for i in range(0, level):
            parent_path = os.path.dirname(os.path.abspath(parent_path))
        return parent_path
