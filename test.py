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
import time
a = time.time()
time.sleep(1)
print time.time() - a

pipe = redis_client.pipeline()
pipe.