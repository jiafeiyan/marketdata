# -*- coding: UTF-8 -*-

class csv_tool:
    def __init__(self):
        pass

    @staticmethod
    def covert_to_gbk(data):
        if isinstance(data, dict):
            return {csv_tool.covert_to_gbk(key): csv_tool.covert_to_gbk(value) for key, value in data.iteritems()}
        elif isinstance(data, list):
            return [csv_tool.covert_to_gbk(element) for element in data]
        elif isinstance(data, tuple):
            return csv_tool.covert_to_gbk(list(data))
        elif isinstance(data, unicode):
            return data.encode('gbk')
        else:
            try:
                return data.decode('utf-8').encode('gbk')
            except:
                return data
