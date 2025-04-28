# 导入依赖包
from pyDLI import dli
import pandas as pd
import datetime


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

    # 获取DLI表的分区字段信息，包括分区字段名，字段类型，字段注释
    def get_dli_table_partition_columns(self, database, table):
        sql = 'show create table %s.%s' % (database, table)
        partition_columns = []
        try:
            self.cursor.execute(sql)
            ddl = self.cursor.fetchall()[0][0]
            items = ddl.split('\n')
            for item in items:
                if item.find('PARTITIONED') > -1:
                    partition_columns_list = item.replace('PARTITIONED BY', '').replace('(', '').replace(')',
                                                                                                         '').replace(
                        '`', '').split(',')
                    for partition in partition_columns_list:
                        partition_column = {}
                        infos = partition.split(' ')
                        field_name = infos[1].lower()
                        field_type = infos[2].lower()
                        partition_column['field_name'] = field_name
                        partition_column['field_type'] = field_type
                        partition_columns.append(partition_column)
        except Exception as e:
            print(e)
        return partition_columns

    # 获取DLI表的字段信息，包括字段名，字段类型，字段注释
    def get_dli_table_columns(self, database, table):
        sql = 'desc %s.%s' % (database, table)
        columns = []
        partition_column_list = []
        for partition_column_info in self.get_dli_table_partition_columns(database, table):
            partition_column_list.append(partition_column_info['field_name'])
        try:
            self.cursor.execute(sql)
            columns_info = self.cursor.fetchall()
            for column_info in columns_info:
                column = {}
                if column_info[0].find('Partition') > 0:
                    break
                column['field_name'] = column_info[0].lower()
                column['field_type'] = column_info[1].lower()
                column['field_comment'] = column_info[2]
                columns.append(column)
        except Exception as e:
            print(e)
        return columns

# 连接dli测试
sql = "select * from yishou_data.all_fmys_inventory limit 2"
# 建立数据库连接
client = YsDliSQLCLient(queue='analyst', database='yishou_data')
# 获取表的分区
# client.get_partitions(database='yishou_data', table='all_fmys_inventory')
# 执行sql语句
client.exec_sql(sql)
# 获取全部结果
result = client.fetch_all_dataframe()
print(result)
