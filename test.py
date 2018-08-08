# -*- coding: UTF-8 -*-

import redis
from pprint import pprint

pool = redis.ConnectionPool(host='192.188.188.125',
                            port='16521',
                            password='manyitsys',
                            db=14)
redis_client = redis.Redis(connection_pool=pool)

import pypinyin
from pypinyin import pinyin
print pinyin(unicode("银行", "utf-8"), heteronym=True, style=pypinyin.FIRST_LETTER)
