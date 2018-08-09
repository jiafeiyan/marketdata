# -*- coding: UTF-8 -*-

import json
import time
import redis

import shfemdapi
from md_handler import MdHandler

from utils import Configuration, parse_conf_args, log, mysql, pubsub


def start_md_service(context, conf):
    logger = log.get_logger(category="MdService")
    logger.info("[start stock md service with %s] begin" % (json.dumps(conf, encoding="UTF-8", ensure_ascii=False)))

    # 获取API连接地址
    exchange_conf = context.get("exchange").get(conf.get("targetExchangeId"))
    exchange_front_addr = str(exchange_conf["mdAddress"])

    # 获取行情用户
    user_id = conf["userId"]
    password = conf["password"]

    # 建立mysql数据库连接
    mysqlDB = mysql(configs=context.get("mysql")[conf.get("mysqlId")])

    # 建立redis数据库连接
    redis_conf = context.get("redis").get(conf.get("redisId"))
    pool = redis.ConnectionPool(host=redis_conf.get('host'),
                                port=redis_conf.get('port'),
                                password=redis_conf.get('password'),
                                db=redis_conf.get('db'))
    redis_client = redis.Redis(connection_pool=pool)

    # 建立API对象
    md_api = shfemdapi.CShfeFtdcMduserApi_CreateFtdcMduserApi()
    md_handler = MdHandler(api=md_api,
                           uid=user_id,
                           pwd=password,
                           mysql=mysqlDB,
                           redis=redis_client,
                           settlementgroup=conf.get("settlementgroup"),
                           exchange=conf.get("exchange"),
                           file=conf.get("filepath"))

    sql = "select TopicID from siminfo.t_marketdatatopic where SettlementGroupID = %s"
    result = mysqlDB.select(sql, (conf.get("settlementgroup"), ))
    for topic_id in result:
        md_api.SubscribeMarketDataTopic(topic_id[0], shfemdapi.TERT_QUICK)

    md_api.RegisterFront(exchange_front_addr)
    md_api.RegisterSpi(md_handler)

    md_api.Init()

    while not md_handler.is_login:
        time.sleep(1)

    md_api.Join()


def main():
    base_dir, config_names, config_files, add_ons = parse_conf_args(__file__, config_names=["xmq", "redis", "mysql", "exchange"])

    context, conf = Configuration.load(base_dir=base_dir, config_names=config_names, config_files=config_files)

    start_md_service(context=context, conf=conf)


if __name__ == "__main__":
    main()
