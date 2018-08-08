# -*- coding: UTF-8 -*-

import os
import json

import path_tool


class Configuration:
    @staticmethod
    def load(base_dir, config_names, config_files, add_ons={}):
        context = {}
        conf = {}

        config_base_dir = base_dir

        if config_base_dir is None:
            default_base_dir = path_tool.path.parent(__file__, 3)
            # 项目路径【默认为当程序路径】
            config_base_dir = os.environ.get("SIM_PLATFORM_HOME", default_base_dir)
            config_base_dir = os.path.join(config_base_dir, "configuration")

        if config_base_dir is not None and config_names is not None:
            for config_name in config_names:
                if config_name.find(":") == -1:
                    config_file = os.path.join(config_base_dir, os.environ.get("SIM_RELEASE"), config_name + ".json")
                    context.update({config_name: Configuration.load_json(config_file)})
                else:
                    config_name_items = config_name.split(":", 1)
                    config_file = os.path.join(config_base_dir, os.environ.get("SIM_RELEASE"), config_name_items[1] + ".json")
                    context.update({config_name_items[0]: Configuration.load_json(config_file)})

        if config_files is not None:
            for config_file in config_files:
                if config_file is not None:
                    # 判断conf是否根目录开始
                    if not config_file.startswith(os.getcwd()[0:3]):
                        config_file = os.path.join(config_base_dir, os.environ.get("SIM_RELEASE"), config_file)
                    if os.path.exists(config_file):
                        conf.update(Configuration.load_json(config_file))
                    else:
                        print("can not find customize config file ==> [%s]" % config_file)
                        exit(-1)

        if add_ons is not None:
            conf.update(add_ons)

        return context, conf

    # 1、查询同级目录下配置文件
    # 2、检查环境变量指向配置文件
    @staticmethod
    def find_selfconfig(file_path, has_config):
        self_config_file = file_path[:-3] + ".json"
        check_files = [self_config_file]
        if os.path.exists(self_config_file):
            return self_config_file
        elif os.environ.get("SIM_RELEASE") is not None:
            # 加载系统版本配置分支
            release = os.environ.get("SIM_RELEASE")
            default_base_dir = path_tool.path.parent(__file__, 3).replace("\\", "/")
            config_base_dir = os.path.join(default_base_dir, "configuration")
            config_base_dir = os.path.join(config_base_dir, release).replace("\\", "/")
            self_config_file = self_config_file.replace("\\", "/")
            diff = self_config_file.replace(default_base_dir, "")
            diff = diff[1:] if diff[0] == '/' else diff
            config_base_dir = os.path.join(config_base_dir, diff)
            check_files.append(config_base_dir)
            if os.path.exists(config_base_dir):
                return config_base_dir
        print("can not find config file ==> %s" % check_files)
        if has_config:
            print("has_customize_config, countinue")
        else:
            exit()

    @staticmethod
    def load_json(file_name):
        with open(file_name) as f:
            config = json.load(f)
            return config

