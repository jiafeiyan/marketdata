# -*- coding: UTF-8 -*-

import argparse
import json

from utils.config.config_tool import Configuration


def parse_conf_args(file_name, base_dir=None, config_names=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-base')
    parser.add_argument('-imp', nargs='+')
    parser.add_argument('-conf', nargs='+')
    parser.add_argument('-ads')
    args = parser.parse_args()
    if args.conf is None:
        args.conf = []

    args.conf.insert(0, Configuration.find_selfconfig(file_name, False if len(args.conf) == 0 else True ))

    ads = {}
    if args.ads is not None:
        ads = json.loads(args.ads)

    return base_dir if args.base is None else args.base, config_names if args.imp is None else args.imp, args.conf, ads
