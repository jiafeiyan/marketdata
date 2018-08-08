# -*- coding: UTF-8 -*-
import datetime
import json
import sys


class RedisAdvMdResolver():

    def __init__(self, **kwargs):
        self.redis = kwargs.get("redis")
        self.mysql = kwargs.get("mysql")
        self.sgid = kwargs.get("sgid")
        self.exchange = kwargs.get("exchange")
        self.tradingday = kwargs.get("tradingDay")

        self.xmq_instrument = kwargs.get("xmq").get("UpdateInstrument")
        self.xmq_market_list = kwargs.get("xmq").get("UpdateMarketList")

        self.advKeyPrefix = self.sgid + ":" + self.tradingday + ":ADV:"
        self.rollKeyPrefix = self.sgid + ":" + "Rolling:ADV:Security:"

    def handle_num_to_str(self, obj, isVolume=False):
        if type(obj) == int or type(obj) == float:
            # 当前为整数，如果保留两位小数修改0.2f
            if isVolume:
                return str(format(float(obj), '0.0f'))
            else:
                return str(format(float(obj), '0.2f'))
        return obj

    # K线图以及行情统一接口
    def unify_md(self, depthMD):
        # try:
        # 计算分钟行情
        self.resolve_minute_md(depthMD)
        # 计算所有K线图信息
        security = depthMD.InstrumentID
        self.resolve_5k_md(security)
        self.resolve_hk_md(security)
        self.resolve_dk_md(security)
        self.resolve_wk_md(security)
        # 发送到消息服务器
        self.xmq_instrument.send({"ID": depthMD.InstrumentID, "TYPE": "tora", "SGID": self.sgid})
        # except Exception as exp:
        #     print(exp)

    # 行情信息
    def resolve_instrument_md(self, depthMD):
        if depthMD is not None:
            advKey = self.advKeyPrefix + "Security:" + depthMD.InstrumentID + ":MarketList"

            currentPrice = self.get_price(depthMD.LastPrice, depthMD.PreClosePrice)
            PreClosePrice = float(depthMD.PreClosePrice)
            # 计算涨跌
            zd = currentPrice - PreClosePrice
            # 计算涨跌幅
            zdf = "0.00%" if PreClosePrice == 0 else ("%.2f" % (zd * 100 / PreClosePrice)) + "%"
            # 计算振幅
            zf = ("0.00" if PreClosePrice == 0 else "%.2f" % (
                    (float(depthMD.HighestPrice) - float(depthMD.LowestPrice)) * 100 / PreClosePrice)) + "%"
            # 计算均价
            jj = ("%.2f" % (
                PreClosePrice if float(depthMD.Volume) == 0 else (float(depthMD.Turnover) / float(depthMD.Volume))))

            advValue = {
                "ID": depthMD.InstrumentID,  # 合约代码
                "ZXJ": self.handle_num_to_str(self.get_price(depthMD.LastPrice, "--")),  # 最新价
                "ZSP": self.handle_num_to_str(self.get_price(depthMD.PreClosePrice, "--")),  # 昨收盘
                "JKP": self.handle_num_to_str(self.get_price(depthMD.OpenPrice, "--")),  # 今开盘
                "ZGJ": self.handle_num_to_str(self.get_price(depthMD.HighestPrice, "--")),  # 最高价
                "ZDJ": self.handle_num_to_str(self.get_price(depthMD.LowestPrice, "--")),  # 最低价
                "ZTJ": self.handle_num_to_str(self.get_price(depthMD.UpperLimitPrice, "--")),  # 涨停板价
                "DTJ": self.handle_num_to_str(self.get_price(depthMD.LowerLimitPrice, "--")),  # 跌停板价
                "BP1": self.handle_num_to_str(self.get_price(depthMD.BidPrice1, "--")),  # 申买价一
                "BV1": self.handle_num_to_str(self.get_price(depthMD.BidVolume1, "0"), True),  # 申买量一
                "AP1": self.handle_num_to_str(self.get_price(depthMD.AskPrice1, "--")),  # 申卖价一
                "AV1": self.handle_num_to_str(self.get_price(depthMD.AskVolume1, "0"), True),  # 申卖量一
                "BP2": self.handle_num_to_str(self.get_price(depthMD.BidPrice2, "--")),  # 申买价二
                "BV2": self.handle_num_to_str(self.get_price(depthMD.BidVolume2, "0"), True),  # 申买量二
                "AP2": self.handle_num_to_str(self.get_price(depthMD.AskPrice2, "--")),  # 申卖价二
                "AV2": self.handle_num_to_str(self.get_price(depthMD.AskVolume2, "0"), True),  # 申卖量二
                "BP3": self.handle_num_to_str(self.get_price(depthMD.BidPrice3, "--")),  # 申买价三
                "BV3": self.handle_num_to_str(self.get_price(depthMD.BidVolume3, "0"), True),  # 申买量三
                "AP3": self.handle_num_to_str(self.get_price(depthMD.AskPrice3, "--")),  # 申卖价三
                "AV3": self.handle_num_to_str(self.get_price(depthMD.AskVolume3, "0"), True),  # 申卖量三
                "BP4": self.handle_num_to_str(self.get_price(depthMD.BidPrice4, "--")),  # 申买价四
                "BV4": self.handle_num_to_str(self.get_price(depthMD.BidVolume4, "0"), True),  # 申买量四
                "AP4": self.handle_num_to_str(self.get_price(depthMD.AskPrice4, "--")),  # 申卖价四
                "AV4": self.handle_num_to_str(self.get_price(depthMD.AskVolume4, "0"), True),  # 申卖量四
                "BP5": self.handle_num_to_str(self.get_price(depthMD.BidPrice5, "--")),  # 申买价五
                "BV5": self.handle_num_to_str(self.get_price(depthMD.BidVolume5, "0"), True),  # 申买量五
                "AP5": self.handle_num_to_str(self.get_price(depthMD.AskPrice5, "--")),  # 申卖价五
                "AV5": self.handle_num_to_str(self.get_price(depthMD.AskVolume5, "0"), True),  # 申卖量五
                "CJL": self.handle_num_to_str(self.get_price(depthMD.Volume, "--")),  # 成交量
                "CJE": self.handle_num_to_str(self.get_price(depthMD.Turnover, "--")),  # 成交额
                "ZD": self.handle_num_to_str(zd),  # 涨跌
                "ZDF": zdf,  # 涨跌幅
                "ZF": zf,  # 振幅
                "JJ": jj,  # 均价
            }
            self.redis.hmset(advKey, advValue)
            self.xmq_market_list.send({"ID": depthMD.InstrumentID, "TYPE": "tora", "SGID": self.sgid})

    def resolve_minute_md(self, depthMD):
        if depthMD is not None:
            tradingday = self.tradingday
            security = depthMD.InstrumentID
            pipe = self.redis.pipeline()

            advKey = self.advKeyPrefix + "Security:" + security + ":MI_MD"
            marketDate = tradingday + depthMD.UpdateTime.replace(":", "")
            score = float(self.calcMDdate(marketDate, str(depthMD.UpdateMillisec)))

            last_min_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
            if len(last_min_md) == 0:
                cj = self.handle_num_to_str(depthMD.Volume)
            else:
                last_min_md_data = json.loads(last_min_md[0][0], encoding="UTF-8")
                last_min_md_score = last_min_md[0][1]
                # 判断当前时间是否大于缓存数据
                if score > last_min_md_score:
                    cj = self.handle_num_to_str(float(depthMD.Volume) - float(last_min_md_data['CJL']))
                else:
                    cj = self.handle_num_to_str(
                        float(depthMD.Volume) - float(last_min_md_data['CJL']) + float(last_min_md_data['CJ']))

            # 计算持仓量，成交量，现价，均价
            ccl = self.handle_num_to_str(self.get_price(depthMD.OpenInterest, "0"), True)
            cjl = self.handle_num_to_str(self.get_price(depthMD.Volume, "0"), True)
            xj = self.handle_num_to_str(self.get_price(depthMD.LastPrice, depthMD.PreClosePrice))
            jj = ("%.2f" % (float(depthMD.PreClosePrice) if float(depthMD.Volume) == 0 else (
                    float(depthMD.Turnover) / float(depthMD.Volume))))

            # 计算涨跌，涨跌幅
            currentPrice = self.get_price(depthMD.LastPrice, depthMD.PreClosePrice)
            PreClosePrice = float(depthMD.PreClosePrice)
            zd = currentPrice - PreClosePrice
            zdf = "0.00%" if PreClosePrice == 0 else ("%.2f" % (zd * 100 / PreClosePrice)) + "%"

            showTime = self.calcMDdate(marketDate, str(depthMD.UpdateMillisec))

            data = {
                "ID": depthMD.InstrumentID,  # 股票代码
                "SJ": showTime,  # 时间
                "SJD": showTime,  # 时间点
                "TIME": marketDate + str(depthMD.UpdateMillisec),  # 记录行情时间戳
                "SF": showTime[8:10] + ":" + showTime[10:12],  # 时分
                "CCL": ccl,  # 持仓量
                "CJL": cjl,  # 成交量
                "CJ": cj,  # 成交
                "XJ": xj,  # 现价
                "JJ": jj,  # 均价
                "ZD": ("%.2f" % zd),  # 涨跌
                "ZDF": zdf  # 涨跌幅
            }

            advValue = json.dumps(data, ensure_ascii=False)
            pipe.zremrangebyscore(advKey, score, score)
            pipe.zadd(advKey, advValue, str(score))
            pipe.execute()

    # 5K行情
    def resolve_5k_md(self, security):
        # 查询分钟行情
        advKey = self.advKeyPrefix + "Security:" + security + ":MI_MD"
        last_min_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
        last_min_md_data = json.loads(last_min_md[0][0], encoding="UTF-8")

        # 查询5K行情
        advKey = self.rollKeyPrefix + security + ":5K_MD"
        last_5K_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
        if len(last_5K_md) == 0:
            last_5K_md_score = None
        else:
            last_5K_md_data = last_5K_md[0][0]
            last_5K_md_score = last_5K_md[0][1]

        time = last_min_md_data["SJ"]
        # 调整分钟时间为5K整点
        if int(time[11:12]) not in (0, 5):
            if int(time[10:12]) > 55:
                calcTime = str(int(time[8:10]) + 1).zfill(2) + "0000"
            else:
                calcTime = time[8:10] + str((int(time[10:12]) / 5 + 1) * 5).zfill(2) + "00"
            time = time[0:8] + calcTime

        # 计算K值
        if time == last_5K_md_score:
            res = self.handle_kdata(last_5K_md_data, last_min_md_data)
            advValue = json.dumps({
                "ID": security,
                "KP": res["OpenPrice"],
                "SP": res["ClosePrice"],
                "ZGJ": res["HighestPrice"],
                "ZDJ": res["LowestPrice"],
                "XL": res["TotalVolume"],
                "SJ": last_min_md_data["SJ"],
                "SJD": time,
            })
        else:
            advValue = json.dumps({
                "ID": security,
                "KP": last_min_md_data["XJ"],
                "SP": last_min_md_data["XJ"],
                "ZGJ": last_min_md_data["XJ"],
                "ZDJ": last_min_md_data["XJ"],
                "XL": last_min_md_data["CJ"],
                "SJ": last_min_md_data["SJ"],
                "SJD": time,
            })
        # 存入redis
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(advKey, time, time)
        pipe.zadd(advKey, advValue, time)
        pipe.execute()

    # 时K行情
    def resolve_hk_md(self, security):
        # 查询分钟行情
        advKey = self.advKeyPrefix + "Security:" + security + ":MI_MD"
        last_min_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
        last_min_md_data = json.loads(last_min_md[0][0], encoding="UTF-8")

        # 查询时K行情
        advKey = self.rollKeyPrefix + security + ":HK_MD"
        last_HK_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)

        if len(last_HK_md) == 0:
            last_HK_md_score = None
        else:
            last_HK_md_data = last_HK_md[0][0]
            last_HK_md_score = last_HK_md[0][1]

        # 查询休市时间
        tradingTime = self.redis.zrange(
            "%s%s%s%s" % (self.advKeyPrefix, "Security:", security, ":TradingTime"), 0, -1)
        noonTime = json.loads(tradingTime[0])["JS"].decode('GBK').encode('UTF-8')[8:12]  # 中午休市时间 11:30

        time = last_min_md_data["SJ"]
        if time[8:12] != noonTime and time[10:12] != "00":
            time = time[0:8] + str(int(time[8:10]) + 1).zfill(2) + "0000"

        if time == last_HK_md_score:
            res = self.handle_kdata(last_HK_md_data, last_min_md_data)
            advValue = json.dumps({
                "ID": security,
                "KP": res["OpenPrice"],
                "SP": res["ClosePrice"],
                "ZGJ": res["HighestPrice"],
                "ZDJ": res["LowestPrice"],
                "XL": res["TotalVolume"],
                "SJ": last_min_md_data["SJ"],
                "SJD": time,
            })
        else:
            advValue = json.dumps({
                "ID": security,
                "KP": last_min_md_data["XJ"],
                "SP": last_min_md_data["XJ"],
                "ZGJ": last_min_md_data["XJ"],
                "ZDJ": last_min_md_data["XJ"],
                "XL": last_min_md_data["CJ"],
                "SJ": last_min_md_data["SJ"],
                "SJD": time,
            })
        # 存入redis
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(advKey, time, time)
        pipe.zadd(advKey, advValue, time)
        pipe.execute()

    # 日K行情
    def resolve_dk_md(self, security):
        # 查询分钟行情
        advKey = self.advKeyPrefix + "Security:" + security + ":MI_MD"
        last_min_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
        last_min_md_data = json.loads(last_min_md[0][0], encoding="UTF-8")

        # 查询日K行情
        advKey = self.rollKeyPrefix + security + ":DK_MD"
        last_DK_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)

        if len(last_DK_md) == 0:
            last_DK_md_score = None
        else:
            last_DK_md_data = last_DK_md[0][0]
            last_DK_md_score = last_DK_md[0][1]

        if self.tradingday == last_DK_md_score:
            res = self.handle_kdata(last_DK_md_data, last_min_md_data)
            advValue = json.dumps({
                "ID": security,
                "KP": res["OpenPrice"],
                "SP": res["ClosePrice"],
                "ZGJ": res["HighestPrice"],
                "ZDJ": res["LowestPrice"],
                "XL": res["TotalVolume"],
                "SJ": self.tradingday,
                "SJD": self.tradingday,
            })
        else:
            advValue = json.dumps({
                "ID": security,
                "KP": last_min_md_data["XJ"],
                "SP": last_min_md_data["XJ"],
                "ZGJ": last_min_md_data["XJ"],
                "ZDJ": last_min_md_data["XJ"],
                "XL": last_min_md_data["CJ"],
                "SJ": self.tradingday,
                "SJD": self.tradingday,
            })
        # 存入redis
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(advKey, self.tradingday, self.tradingday)
        pipe.zadd(advKey, advValue, self.tradingday)
        pipe.execute()

    # 周K行情
    def resolve_wk_md(self, security):
        # 查询分钟行情
        advKey = self.advKeyPrefix + "Security:" + security + ":MI_MD"
        last_min_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)
        last_min_md_data = json.loads(last_min_md[0][0], encoding="UTF-8")

        # 查询周K行情
        advKey = self.rollKeyPrefix + security + ":WK_MD"
        last_WK_md = self.redis.zrange(name=advKey, start=-1, end=-1, withscores=True)

        if len(last_WK_md) == 0:
            last_WK_md_score = None
        else:
            last_WK_md_data = last_WK_md[0][0]
            last_WK_md_score = last_WK_md[0][1]

        time = last_min_md_data["SJ"]

        friday = 4
        diff = friday - datetime.datetime.strptime(time[0:8], "%Y%m%d").weekday()
        date_time = datetime.datetime.strptime(time[0:8], '%Y%m%d')
        marketDate = (date_time + datetime.timedelta(days=diff)).strftime("%Y%m%d")

        if marketDate == last_WK_md_score:
            res = self.handle_kdata(last_WK_md_data, last_min_md_data)
            advValue = json.dumps({
                "ID": security,
                "KP": res["OpenPrice"],
                "SP": res["ClosePrice"],
                "ZGJ": res["HighestPrice"],
                "ZDJ": res["LowestPrice"],
                "XL": res["TotalVolume"],
                "SJ": self.tradingday,
                "SJD": marketDate,
            })
        else:
            advValue = json.dumps({
                "ID": security,
                "KP": last_min_md_data["XJ"],
                "SP": last_min_md_data["XJ"],
                "ZGJ": last_min_md_data["XJ"],
                "ZDJ": last_min_md_data["XJ"],
                "XL": last_min_md_data["CJ"],
                "SJ": self.tradingday,
                "SJD": marketDate,
            })

        # 存入redis
        pipe = self.redis.pipeline()
        pipe.zremrangebyscore(advKey, marketDate, marketDate)
        pipe.zadd(advKey, advValue, marketDate)
        pipe.execute()

    # 处理K线图数据
    def handle_kdata(self, md_data, source_data):
        md_data = json.loads(md_data, encoding="UTF-8")
        res = {"OpenPrice": md_data["KP"],
               "ClosePrice": md_data["SP"],
               "LowestPrice": md_data["ZDJ"],
               "HighestPrice": md_data["ZGJ"],
               "TotalVolume": md_data["XL"]}
        # 判断最高价
        if source_data["XJ"] > res["HighestPrice"]:
            res["HighestPrice"] = source_data["XJ"]
        # 判断最低价
        if source_data["XJ"] < res["LowestPrice"]:
            res["LowestPrice"] = source_data["XJ"]
        # 更新收盘价
        res["ClosePrice"] = source_data["XJ"]
        # 更新成交量
        res["TotalVolume"] += int(float(source_data["CJ"]))
        return res

    def calcMDdate(self, date, milliseconds):
        diffMilli = 0
        diffSec = 0
        second = date[12:14]
        if int(milliseconds) != 0:
            diffMilli = 1000 - int(milliseconds)
            diffSec = 1
        if second != "00" or diffSec == 1:
            diffSec = 60 - int(second) - diffSec
        date_time = datetime.datetime.strptime(date + milliseconds, "%Y%m%d%H%M%S%f")
        date = (date_time + datetime.timedelta(seconds=diffSec, milliseconds=diffMilli)).strftime("%Y%m%d%H%M%S")
        return date

    def get_price(self, price, defaultValue):
        if (float(sys.float_info.max) == float(price)) or float(price) == 0:
            if defaultValue == "--":
                return "--"
            else:
                return float(round(float(defaultValue), 2))
        return float(round(float(price), 2))
