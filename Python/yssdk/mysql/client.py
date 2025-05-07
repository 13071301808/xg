# coding=utf-8

from yssdk.common.printter import Printter
from yssdk.common.config import Config
import pymysql
import sys
import traceback
import pandas as pd


class YsMysqlClient:
    def __init__(self, conf_mode='prod'):
        self.__conf = Config.get_config(conf_mode)

    def __del__(self):
        self.close()

    def select(self, sql, args=None):
        """
        查询
        :param sql:
        :param args:
        :return:
        """
        try:
            self.__cursor.execute(query=sql, args=args)
            self.__conn.commit()
            return self.__cursor.fetchall()
        except Exception as e:
            Printter.error(traceback.format_exc())
            sys.exit('执行失败,失败SQL:' + sql)

    def get_columns(self):
        """
        获取字段
        """
        column_list = []
        for column_desc in self.__cursor.description:
            column_list.append(column_desc[0])
        return column_list

    def update(self, sql):
        """
        支持删除、更新、插入的SQL
        :param sql:
        :return:
        """
        try:
            Printter.info("执行SQL:" + sql)
            self.__cursor.execute(sql)
            self.__conn.commit()
        except Exception as e:
            Printter.error(traceback.format_exc())
            sys.exit('执行失败,失败SQL:' + sql)

    def init_connection(self, host, user, password, db, port=3306):
        """
        初始化 Mysql connection对象
        :return: Mysql connection对象
        """
        self.__conn = pymysql.connect(host=host, user=user, password=password, db=db, charset="utf8mb4", read_timeout=300, port=port)
        self.__cursor = self.__conn.cursor()

    def get_connection(self):
        """
        获取 Mysql connection对象
        :return: connection对象
        """
        return self.__conn

    def close(self):
        """
        关闭Mysql connection对象
        :return: None
        """
        try:
            self.__cursor.close()
            self.__conn.close()
        except AttributeError:
            Printter.warn("未能正确关闭游标对象与连接对象")

    def get_r7connect(self):
        '''
        连接r7数据库【mysql】
        :return: connection对象
        '''
        self.__conn = pymysql.connect(host=self.__conf['r7_mysql_conf']['ys_host'],
                                    user=self.__conf['r7_mysql_conf']['ys_user'],
                                    password=self.__conf['r7_mysql_conf']['ys_password'],
                                    db=self.__conf['r7_mysql_conf']['ys_db'])
        self.__cursor = self.__conn.cursor()
        return self.__conn

    def get_zzconnect(self):
        '''
        连接自主取数数据库【mysql】
        :return: connection对象
        '''
        self.__conn = pymysql.connect(host=self.__conf['mysql_conf']['ys_host'],
                                    user=self.__conf['mysql_conf']['ys_user'],
                                    password=self.__conf['mysql_conf']['ys_password'],
                                    db=self.__conf['mysql_conf']['ys_db'])
        self.__cursor = self.__conn.cursor()
        return self.__conn

    def get_obconnect(self):
        '''
        连接自主取数数据库【mysql】
        :return: connection对象
        '''
        self.__conn = pymysql.connect(host=self.__conf['oceanbase_conf']['ys_host'],
                                    user=self.__conf['oceanbase_conf']['ys_user'],
                                    password=self.__conf['oceanbase_conf']['ys_password'],
                                    db=self.__conf['oceanbase_conf']['ys_db'],
                                    port=int(self.__conf['oceanbase_conf']['ys_port']))
        self.__cursor = self.__conn.cursor()
        return self.__conn

    def get_mysql_table_column(self, mysql_db, mysql_table):
        """
        获取mysql表的字段信息
        :param mysql_db:
        :param mysql_table:
        :return:返回字典类型，包含信息，'field_name','field_type','field_comment','key'
        """
        sql_column_desc = "select COLUMN_NAME,DATA_TYPE,COLUMN_COMMENT,COLUMN_KEY from information_schema.COLUMNS where TABLE_SCHEMA = '%s' and TABLE_NAME='%s'"%(mysql_db,mysql_table)
        column_desc_header = ['field_name','field_type','field_comment','key']
        column_desc = self.select(sql=sql_column_desc)
        df = pd.DataFrame(list(column_desc), columns=column_desc_header)
        fields = df.sort_values('field_name').to_dict(orient='records')
        return fields

    def get_table_primary_key(self, mysql_db, mysql_table):
        """
        获取mysql表的主键
        :param mysql_db:
        :param mysql_table:
        :return: 返回字符串类型,联合主键用逗号隔开
        """
        sql_primary_key = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA = '%s' and TABLE_NAME='%s' and COLUMN_KEY = 'PRI'"%(mysql_db,mysql_table)
        primary_key = []
        pk_return = self.select(sql=sql_primary_key)
        for pk in pk_return:
            primary_key.append(pk[0])
        primary_keys = ",".join(primary_key)
        return primary_keys

    def execute_sql(self, sql, output=True):
        if output:
            Printter.info("Execute DLI SQL:" + " ".join(sql.split()))
        self.__cursor.execute(sql)

    def fetch_all(self):
        """
        拉取resultset所有数据
        :return: list类型
        """
        return self.__cursor.fetchall()

    def fetch_one(self):
        """
        拉取resultset一条数据
        :return: list类型
        """
        return self.__cursor.fetchone()

    def commit(self):
        self.__conn.commit()


if __name__ == '__main__':
    pass

