# -*- coding: UTF-8 -*-

from utils.logger.log import log
from mysql.connector import pooling


class mysql:
    def __init__(self, configs):
        self.logger = log.get_logger(category="mysql")
        db_config = {
            "user": configs["user"],
            "password": configs["password"],
            "host": configs["host"],
            "port": configs["port"],
            "database": configs["database"]
        }
        self.__connect(db_config=db_config, pool_size=configs["pool_size"])

    def __connect(self, db_config, pool_size):
        self.logger.info("start connect mysql database [ user=%s, host=%s, port=%s ]",
                         db_config["user"], db_config["host"], db_config["port"])
        self.pool = pooling.MySQLConnectionPool(pool_size=pool_size, **db_config)
        self.logger.info("connect database success")

    def get_cnx(self):
        return self.pool.get_connection()

    # 判断是否存在记录
    def is_exist(self, sql, params):
        res = self.select(sql=sql, params=params)
        if len(res) > 0:
            return True
        else:
            return False

    # 查询
    def select(self, sql, params=None):
        cnx = self.get_cnx()
        try:
            self.logger.debug({"sql": sql, "params": params})
            cursor = cnx.cursor()
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            fc = cursor.fetchall()
            return fc
        except Exception as err:
            self.logger.error(err)
        finally:
            cursor.close()
            cnx.close()

    # 执行
    def execute(self, sql, params=None):
        cnx = self.get_cnx()
        try:
            self.logger.debug({"sql": sql, "params": params})
            cursor = cnx.cursor()
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            self.logger.debug("effect " + str(cursor.rowcount) + " record ")
            cnx.commit()
        except Exception as err:
            self.logger.error(err)
        finally:
            cursor.close()
            cnx.close()

    # 传入多个sql和参数的集合，进行事物执行
    # 数据结构 [dict(sql="", params=())]
    def executetransaction(self, sqls_params):
        cnx = self.get_cnx()
        try:
            cnx.start_transaction()
            cursor = cnx.cursor()
            for data in sqls_params:
                if data['params'] is None:
                    cursor.execute(data['sql'])
                else:
                    cursor.execute(data['sql'], data['params'])
            cnx.commit()
        except Exception as err:
            self.logger.error(err)
        finally:
            cursor.close()
            cnx.close()

    # 批量执行
    def executemany(self, sql, params):
        cnx = self.get_cnx()
        try:
            self.logger.debug({"sql": sql, "params": params})
            cursor = cnx.cursor()
            cursor.executemany(sql, params)
            self.logger.debug("effect " + str(str(cursor.rowcount)) + " record ")
            cnx.commit()
        except Exception as err:
            cnx.rollback()
            self.logger.error(err)
        finally:
            cursor.close()
            cnx.close()
