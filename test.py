# -*- coding: UTF-8 -*-

import redis

from redis import WatchError

# pool = redis.ConnectionPool(host='10.189.65.70',
#                             port='16521',
#                             password='111111',
#                             db=15)

pool = redis.ConnectionPool(host='192.188.188.125',
                            port='16521',
                            password='manyitsys',
                            db=15)

redis_client = redis.Redis(connection_pool=pool)
print redis_client.get("SG01:20180802:ADV:Security:600016:Last_Minute")

a = "20180808"

print len(a)

# def inc_and_get():
#     with redis_client.pipeline(transaction=True) as pipe:
#         while True:
#             try:
#                 pipe.watch(adv)
#                 range_start = 1 if pipe.get(adv) is None else int(pipe.get(adv))
#                 pipe.multi()
#                 range_end = int(range_start) + int(2)
#                 instrument_list = redis_client.zrangebyscore("SG01:20180725:ADV:Security:List", range_start, range_end - 1)
#
#                 if len(instrument_list) < 2:
#                     range_end = 1
#
#                 pipe.set(adv, range_end)
#                 pipe.execute()
#                 return instrument_list
#             except WatchError, ex:
#                 print ex
#                 pipe.unwatch()
#
# print inc_and_get()

# add_time = (old_time + datetime.timedelta(minutes=i)).strftime("%H%M")

# 600063







