import pandas as pd
import psycopg2
from yssdk.common.printter import Printter
from yssdk.common.config import Config
import traceback
import sys

class YsGaussDBClient:
    def __init__(self, conf_mode='prod'):
        conf = Config.get_config(conf_mode)
        self.__db = conf['gaussdb_conf']['ys_db']
        self.__host = conf['gaussdb_conf']['ys_host']
        self.__user = conf['gaussdb_conf']['ys_user']
        self.__password = conf['gaussdb_conf']['ys_password']
        self.__port = conf['gaussdb_conf']['ys_port']

        Printter.info("Init GaussDB Connection...")
        self.conn = psycopg2.connect(database=self.__db, user=self.__user, password=self.__password, host=self.__host,
                            port=self.__port)
        self.conn.set_client_encoding('UTF-8')
        self.cursor = self.conn.cursor()

    def get_connection(self):
        return self.conn

    def select(self, sql):
        try:
            self.cursor.execute(query=sql)
            return self.cursor.fetchall()
        except Exception as e:
            Printter.error(traceback.format_exc())
            sys.exit('执行失败,失败SQL:' + sql)

    # 删除、更新、插入
    def update(self, sql):
        try:
            Printter.info("执行SQL:" + sql)
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            Printter.error(traceback.format_exc())
            sys.exit('执行失败,失败SQL:' + sql)

    def exec_sql_return_dataframe(self, sql):
        Printter.info(sql)
        try:
            self.cursor.execute(sql)
            column_list = []
            for column_desc in self.cursor.description:
                column_list.append(column_desc[0])
            data = list(self.cursor.fetchall())
            return pd.DataFrame(data, columns=column_list)
        except Exception as e:
            Printter.error(traceback.format_exc())

    def close(self):
        self.conn.close()

    def __del__(self):
        self.close()

if __name__ == '__main__':
    pass


