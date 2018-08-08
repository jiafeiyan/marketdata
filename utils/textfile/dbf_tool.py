# -*- coding: UTF-8 -*-

from dbfread import DBF
from pprint import pprint


table = DBF("E:/GithubResponsity/my_python_util/test/PAR_STOCK20171228.dbf", encoding="GBK")

table.load()

pprint(table.records[0])
