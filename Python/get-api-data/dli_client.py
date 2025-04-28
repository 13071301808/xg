# 导入依赖包
from pyDLI import dli
import pandas as pd


class YsDliSQLCLient:
    # 初始化
    def __init__(self, queue, database):
        endpoint = "dli.cn-south-1.myhuaweicloud.com"
        project_id = "0ba6e278cc80f3562f48c00242b9c5d8"
        ak = "BOYRQSQ8XUPWJLLKZZSO"
        sk = "1zeUjOpGgXldkdsTQZLiFi48lGkR9V0ort65q1oy"
        auth_mode = "aksk"
        queue = queue
        database = database
        print('dli连接中。。。')
        self.conn = dli.Connection(
            host="dli://%s/%s?queuename=%s&database=%s" % (endpoint, project_id, queue, database),
            ak=ak,
            sk=sk,
            auth=auth_mode
        )
        self.cursor = self.conn.cursor()

    # 执行单条 DLI SQL
    def exec_sql(self, sql, parameters=None, options=None):
        default_options = {'dli.sql.dynamicPartitionOverwrite.enabled': 'true'}
        sql_options = default_options.copy()
        if options is not None:
            sql_options.update(options)
        print("执行sql语句:" + " ".join(sql.split()))
        try:
            self.cursor.execute(sql, parameters, sql_options)
        except Exception as e:
            print(e)

    # 获取所有数据，列表形式
    def fetch_all(self):
        return self.cursor.fetchall()

    # 返回resultset的所有数据，并转化为pandas dataframe对象
    def fetch_all_dataframe(self):
        try:
            column_list = [column.name for column in self.cursor._sql_job.get_schema()]
            data = list(self.cursor.fetchall())
            return pd.DataFrame(data, columns=column_list)
        except Exception as e:
            print(e)

    # 获取表的分区
    def get_partitions(self, database, table):
        print("获取%s.%s的分区" % (database, table))
        try:
            sql = "show partitions %s.%s" % (database, table)
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print(e)
