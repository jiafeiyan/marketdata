# -*- coding: UTF-8 -*-

import redis
import json

pool = redis.ConnectionPool(host='192.188.188.125',
                            port='16521',
                            password='manyitsys',
                            db=14)
redis_client = redis.Redis(connection_pool=pool)


file = '/Users/chenyan/CompanyProjects/marketdata/marketdata/securites_abbr.csv'

# with open(file) as f:
#     for line in f:
#         print line.replace("\r\n", "").split(",")
import pypinyin
from pypinyin import pinyin
a = json.dumps(pinyin(unicode("行", "utf-8"), heteronym=True, style=pypinyin.FIRST_LETTER))
print type(a)

a = "PFYH"
res = [[]]
for i in a:
    res[0].append(i.lower())
print type(res)