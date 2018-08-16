# -*- coding: UTF-8 -*-

import threading
import shfemdapi
import time
import pypinyin

from RedisAdvMdResolver import *
from utils import log, path
from pypinyin import pinyin


class MdHandler(shfemdapi.CShfeFtdcMduserSpi):
    ADV_KEY_PREFIX = ":ADV:"

    def __init__(self, **kwargs):
        self.logger = log.get_logger(category="MdSpi")
        shfemdapi.CShfeFtdcMduserSpi.__init__(self)

        self.api = kwargs.get("api")
        self.uid = kwargs.get("uid")
        self.pwd = kwargs.get("pwd")
        self.mysql = kwargs.get("mysql")
        self.redis_raw = kwargs.get("redis_raw")
        self.redis_adv = kwargs.get("redis_adv")
        self.exchange = kwargs.get("exchange")
        self.sgid = kwargs.get("settlementgroup")
        self.file = kwargs.get("file")

        self.is_connected = False
        self.is_login = False

        self.context = {}
        self.threadContext = {}

        self.request_id = 0
        self.lock = threading.Lock()

        # 缓存csv数据
        self.attr = dict()
        if self.file is not None:
            with open(path.convert(self.file)) as f:
                for line in f:
                    line = line.replace("\r\n", "").split(",")
                    self.attr.update({line[0]: line[1]})

    def OnFrontConnected(self):
        self.logger.info("OnFrontConnected")
        self.is_connected = True

        req_login_field = shfemdapi.CShfeFtdcReqUserLoginField()
        req_login_field.UserID = str(self.uid)
        req_login_field.Password = str(self.pwd)

        self.api.ReqUserLogin(req_login_field, self.get_request_id())

    def OnFrontDisconnected(self, nReason):
        self.logger.info("OnFrontDisconnected: %s" % str(nReason))
        self.is_connected = False

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        self.logger.info("OnRspUserLogin")

        if pRspInfo is not None and pRspInfo.ErrorID != 0:
            self.logger.error("login failed : %s" % pRspInfo.ErrorMsg.decode("GBK").encode("UTF-8"))
            time.sleep(3)
            req_login_field = shfemdapi.CShfeFtdcReqUserLoginField()
            req_login_field.UserID = str(self.uid)
            req_login_field.Password = str(self.pwd)
            self.is_login = False
            self.api.ReqUserLogin(req_login_field, self.get_request_id())
        else:
            self.logger.info("%s login success", pRspUserLogin.UserID)
            self.is_login = True
            self.context["loadTradingDay"] = True
            self.context["securityIDs"] = {}
            self.context["securityList"] = []  # 判断是否存在写入交易时间段
            self.context["dayEndSecurity"] = []  # 存放当天交易日结束的股票，防止重复计算
            self.threadContext = {}

    def OnRspSubscribeTopic(self, pDissemination, pRspInfo, nRequestID, bIsLast):
        self.logger.info("OnRspSubscribeTopic")
        if pRspInfo is not None and pRspInfo.ErrorID != 0:
            self.logger.error("OnRspSubscribeTopic failed : %s" % pRspInfo.ErrorMsg.decode("GBK").encode("UTF-8"))
        else:
            print("SequenceSeries ==> " + str(pDissemination.SequenceSeries))
            print("SequenceNo ==> " + str(pDissemination.SequenceNo))

    def OnRtnDepthMarketData(self, pDepthMarketData):
        if pDepthMarketData is not None:
            # 判断是否第一次onRspUserLogin
            if self.context["loadTradingDay"]:
                # 写入TradingDay
                tradingDay = self.context["tradingDay"] = pDepthMarketData.TradingDay
                self.redis_adv.set(self.sgid + ":Exchange:TradingDay", tradingDay)
                self.context["loadTradingDay"] = False
                self.logger.info("TradingDay: %s", tradingDay)

            # self.logger.info("depth data: " + pDepthMarketData.UpdateTime + " || ExchangeID：" +
            #                str(self.exchange) + " || SecurityID：" + pDepthMarketData.InstrumentID)

            # 判断股票代码是否在List缓存中，如果不是写入redis与缓存
            if pDepthMarketData.InstrumentID not in self.context["securityList"]:
                self.context["securityList"].append(pDepthMarketData.InstrumentID)
                advListKey = self.sgid + ":" + self.context["tradingDay"] + self.ADV_KEY_PREFIX + "Security:List"
                self.lock.acquire()
                try:
                    pipe = self.redis_adv.pipeline()
                    # 获取结果集长度
                    size = len(self.context["securityList"])
                    pipe.zremrangebyscore(advListKey, size, size)
                    pipe.zadd(advListKey, pDepthMarketData.InstrumentID, size)
                    pipe.execute()
                finally:
                    self.lock.release()

                # 写入交易时间段缓存
                self.resolve_segment(pDepthMarketData.InstrumentID)
                # 判断是否有MarketList
                self.createMarketList(pDepthMarketData.InstrumentID)

            securityID = pDepthMarketData.InstrumentID

            depthMDDict = {"TradingDay": pDepthMarketData.TradingDay,
                           "SettlementGroupID": pDepthMarketData.SettlementGroupID,
                           "SettlementID": pDepthMarketData.SettlementID,
                           "LastPrice": pDepthMarketData.LastPrice,
                           "PreSettlementPrice": pDepthMarketData.PreSettlementPrice,
                           "PreClosePrice": pDepthMarketData.PreClosePrice,
                           "PreOpenInterest": pDepthMarketData.PreOpenInterest,
                           "OpenPrice": pDepthMarketData.OpenPrice,
                           "HighestPrice": pDepthMarketData.HighestPrice,
                           "LowestPrice": pDepthMarketData.LowestPrice,
                           "Volume": pDepthMarketData.Volume,
                           "Turnover": pDepthMarketData.Turnover,
                           "OpenInterest": pDepthMarketData.OpenInterest,
                           "ClosePrice": pDepthMarketData.ClosePrice,
                           "SettlementPrice": pDepthMarketData.SettlementPrice,
                           "UpperLimitPrice": pDepthMarketData.UpperLimitPrice,
                           "LowerLimitPrice": pDepthMarketData.LowerLimitPrice,
                           "PreDelta": pDepthMarketData.PreDelta,
                           "CurrDelta": pDepthMarketData.CurrDelta,
                           "UpdateTime": pDepthMarketData.UpdateTime,
                           "UpdateMillisec": pDepthMarketData.UpdateMillisec,
                           "InstrumentID": pDepthMarketData.InstrumentID,
                           "BidPrice1": pDepthMarketData.BidPrice1,
                           "BidVolume1": pDepthMarketData.BidVolume1,
                           "AskPrice1": pDepthMarketData.AskPrice1,
                           "AskVolume1": pDepthMarketData.AskVolume1,
                           "BidPrice2": pDepthMarketData.BidPrice2,
                           "BidVolume2": pDepthMarketData.BidVolume2,
                           "AskPrice2": pDepthMarketData.AskPrice2,
                           "AskVolume2": pDepthMarketData.AskVolume2,
                           "BidPrice3": pDepthMarketData.BidPrice3,
                           "BidVolume3": pDepthMarketData.BidVolume3,
                           "AskPrice3": pDepthMarketData.AskPrice3,
                           "AskVolume3": pDepthMarketData.AskVolume3,
                           "BidPrice4": pDepthMarketData.BidPrice4,
                           "BidVolume4": pDepthMarketData.BidVolume4,
                           "AskPrice4": pDepthMarketData.AskPrice4,
                           "AskVolume4": pDepthMarketData.AskVolume4,
                           "BidPrice5": pDepthMarketData.BidPrice5,
                           "BidVolume5": pDepthMarketData.BidVolume5,
                           "AskPrice5": pDepthMarketData.AskPrice5,
                           "AskVolume5": pDepthMarketData.AskVolume5,
                           "ActionDay": pDepthMarketData.ActionDay}

            if pDepthMarketData.UpdateMillisec < 0:
                depthMDDict["BidVolume1"] = 0
                depthMDDict["BidVolume2"] = 0
                depthMDDict["BidVolume3"] = 0
                depthMDDict["BidVolume4"] = 0
                depthMDDict["BidVolume5"] = 0
                depthMDDict["AskVolume1"] = 0
                depthMDDict["AskVolume2"] = 0
                depthMDDict["AskVolume3"] = 0
                depthMDDict["AskVolume4"] = 0
                depthMDDict["AskVolume5"] = 0
                depthMDDict["UpdateMillisec"] = 0

            rawKeyPrefix = self.sgid + ":" + self.context["tradingDay"] + ":RAW:Security:" + securityID + ":LS_MD:"
            key = "%s%s%s%s" % (
                rawKeyPrefix, depthMDDict["UpdateTime"], ":", str(depthMDDict["UpdateMillisec"]).zfill(3))

            self.redis_raw.hmset(key, depthMDDict)

            rawKey = rawKeyPrefix + "List"
            rawValue = key
            score = depthMDDict["UpdateTime"].replace(":", "") + str(depthMDDict["UpdateMillisec"]).zfill(3)
            self.redis_raw.zadd(rawKey, rawValue, score)

    def get_request_id(self):
        self.lock.acquire()
        self.request_id += 1
        req_id = self.request_id
        self.lock.release()
        return req_id

    # 获取合约交易节
    def resolve_segment(self, SecurityID):
        tradingDay = self.context["tradingDay"]
        rawKeyPrefix = self.sgid + ":" + tradingDay + ":RAW:"
        advKeyPrefix = self.sgid + ":" + tradingDay + ":ADV:"
        sql = """SELECT
                    SettlementGroupID,
                    TradingSegmentSN,
                    TradingSegmentName,
                    StartTime,
                    InstrumentStatus,
                    InstrumentID 
                FROM
                    siminfo.t_tradingsegmentattr 
                WHERE
                    SettlementGroupID = %s
                    and InstrumentID = %s 
                ORDER BY
                    TradingSegmentSN"""
        result = self.mysql.select(sql, (self.sgid, SecurityID,))
        # 缓存连续交易开始、结束时间点，并在收到收盘时间点时保存到redis
        segs = []
        for index, (SettlementGroupID,
                    TradingSegmentSN,
                    TradingSegmentName,
                    StartTime,
                    InstrumentStatus,
                    InstrumentID) in enumerate(result):
            segmentDict = {"InstrumentID": str(InstrumentID), "SettlementGroupID": str(SettlementGroupID),
                           "TradingSegmentSN": str(TradingSegmentSN), "TradingSegmentName": str(TradingSegmentName),
                           "StartTime": str(StartTime), "InstrumentStatus": str(InstrumentStatus)}
            # 写入Raw记录
            self.redis_raw.hmset(
                "%s%s%s%s%s" % (rawKeyPrefix, "Security:", SecurityID, ":SEG:", str(TradingSegmentSN).zfill(3)),
                segmentDict)
            # 写入Adv记录
            if InstrumentStatus == '2':
                iStartTime = StartTime.strip().replace(":", "")
                segs.append({"InstrumentID": SecurityID, "StartTime": tradingDay + iStartTime,
                             "ShowStartTime": StartTime.strip()})
            if InstrumentStatus == '1':
                if len(segs) > 0 and "EndTime" not in segs[-1]:
                    iStartTime = StartTime.strip().replace(":", "")
                    segs[-1].update({"EndTime": tradingDay + iStartTime, "ShowEndTime": StartTime.strip()})
            if InstrumentStatus == '6':
                advKey = advKeyPrefix + "Security:" + SecurityID + ":TradingTime"
                pipe = self.redis_adv.pipeline()
                pipe.zremrangebyscore(advKey, 1, 9999)
                for index in range(len(segs)):
                    seg = segs[index]
                    advValue = json.dumps({"TD": tradingDay,
                                           "ID": SecurityID,
                                           "KS": str(seg["StartTime"]),
                                           "SKS": str(seg["ShowStartTime"]),
                                           "JS": str(seg["EndTime"]),
                                           "SJS": str(seg["ShowEndTime"])},
                                          ensure_ascii=False)
                    pipe.zadd(advKey, advValue, index + 1)
                pipe.execute()

    # 判断是否有MarketList
    def createMarketList(self, SecurityID):
        advKeyPrefix = self.sgid + ":" + self.context["tradingDay"] + self.ADV_KEY_PREFIX
        sql = """SELECT
                    t.InstrumentID,
                    t.InstrumentName,
                    t.ProductID,
                    t.SettlementGroupID,
                    t1.PriceTick,
                    t.UnderlyingMultiple,
                    t1.MaxLimitOrderVolume,
                    t1.MinLimitOrderVolume 
                FROM
                    siminfo.t_instrument t,
                    siminfo.t_instrumentproperty t1 
                WHERE
                    t.InstrumentID = t1.InstrumentID 
                    AND t.SettlementGroupID = t1.SettlementGroupID
                    AND t.InstrumentID = %s """
        result = self.mysql.select(sql, (SecurityID,))
        self.is_login = True
        # 插入List记录
        for index, (InstrumentID,
                    InstrumentName,
                    ProductID,
                    SettlementGroupID,
                    PriceTick,
                    VolumeMultiple,
                    MaxLimitOrderVolume,
                    MinLimitOrderVolume) in enumerate(result):

            InstrumentID = str(InstrumentID)
            InstrumentName = str(InstrumentName)
            PriceTick = str(PriceTick)
            VolumeMultiple = str(VolumeMultiple)
            MaxLimitOrderVolume = str(MaxLimitOrderVolume)
            MinLimitOrderVolume = str(MinLimitOrderVolume)

            advKey = advKeyPrefix + "Security:" + SecurityID + ":MarketList"
            advNameKey = advKeyPrefix + "Security:NameList"
            self.redis_adv.hmset(advNameKey, {InstrumentID: InstrumentName})
            res = self.redis_adv.hgetall(advKey)

            if not res:
                advValue = {
                    "ID": SecurityID,  # 合约代码
                    "MZ": InstrumentName,  # 合约名称
                    "SX": self.get_instrument_attr(SecurityID, InstrumentName),  # 缩写
                    "SG": self.sgid,
                    "EX": self.exchange,
                    "ZXJ": "--",  # 最新价
                    "ZSP": "--",  # 昨收盘
                    "JKP": "--",  # 今开盘
                    "ZGJ": "--",  # 最高价
                    "ZDJ": "--",  # 最低价
                    "ZTJ": "--",  # 涨停板价
                    "DTJ": "--",  # 跌停板价
                    "BP1": "--",  # 申买价一
                    "BV1": "0",  # 申买量一
                    "AP1": "--",  # 申卖价一
                    "AV1": "0",  # 申卖量一
                    "BP2": "--",  # 申买价二
                    "BV2": "0",  # 申买量二
                    "AP2": "--",  # 申卖价二
                    "AV2": "0",  # 申卖量二
                    "BP3": "--",  # 申买价三
                    "BV3": "0",  # 申买量三
                    "AP3": "--",  # 申卖价三
                    "AV3": "0",  # 申卖量三
                    "BP4": "--",  # 申买价四
                    "BV4": "0",  # 申买量四
                    "AP4": "--",  # 申卖价四
                    "AV4": "0",  # 申卖量四
                    "BP5": "--",  # 申买价五
                    "BV5": "0",  # 申买量五
                    "AP5": "--",  # 申卖价五
                    "AV5": "0",  # 申卖量五
                    "ZD": "--",  # 涨跌
                    "ZDF": "--",  # 涨跌幅
                    "CJL": "--",  # 成交量
                    "CJE": "--",  # 成交额
                    "ZF": "--",  # 振幅
                    "JJ": "--",  # 均价
                    # ====================================================================
                    "BDJ": PriceTick,  # 最小变动价位
                    "BS": VolumeMultiple,  # 合同数量乘数
                    "ZDL": MaxLimitOrderVolume,  # 限价单最大下单量
                    "ZXL": MinLimitOrderVolume,  # 限价单最小下单量
                    # ====================================================================
                }
                self.redis_adv.hmset(advKey, advValue)

    # 获取名称简称
    def get_instrument_attr(self, security_id, security_name):
        res = [[]]
        if security_id in self.attr:
            ins_attr = self.attr.get(security_id)
            for i in ins_attr:
                res[0].append(i.lower())
        else:
            res = pinyin(unicode(security_name, "utf-8"), heteronym=True, style=pypinyin.FIRST_LETTER)
        return json.dumps(res)
