# 导入依赖包
import time
from datetime import datetime
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkdli.v1.region.dli_region import DliRegion
from huaweicloudsdkdli.v1 import *
from huaweicloudsdkcore.exceptions import exceptions
import json
import pandas as pd
import math
import pymysql

# 设置pandas输出参数
pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', None)  # 设置列数为无限制
pd.set_option('display.width', 1000)  # 设置列宽
pd.set_option('display.colheader_justify', 'left')


# 定义表操作连接
class MysqlClient:
    # 初始化
    def __init__(self, db, host, user, password, timeout=10):
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset="utf8mb4",
                                    connect_timeout=timeout)
        self.cursor = self.conn.cursor()

    # 结束
    def __del__(self):
        self.close()

    # 查询
    def select(self, sql, args):
        self.cursor.execute(query=sql, args=args)
        return self.cursor.fetchall()

    # 删除、更新、插入
    def update(self, sql):
        temp = self.cursor.execute(sql)
        self.conn.commit()
        return temp

    # 关闭连接
    def close(self):
        self.cursor.close()
        self.conn.close()


# 创建华为云api连接
def create_client():
    # ak,sk公钥
    ak = "EULIXLKJQLUS62GPNIAX"
    sk = "vmoTDJVUOXVtwt8pwh5LQ6LnE2bJPDtxid7biimK"
    # 加入认证
    credentials = BasicCredentials(ak, sk)
    # 建立client连接端口
    client = DliClient.new_builder() \
        .with_credentials(credentials) \
        .with_region(DliRegion.value_of("cn-south-1")) \
        .build()
    return client


# 获取数据库
def get_databases():
    # 获取数据库据api接口请求
    request = ListDatabasesRequest()
    # 获取数据库api接口
    response = client.list_databases(request)
    # 设置获取形式并转成json数据
    response = json.dumps(response, default=str, ensure_ascii=False)
    # 对json数据双重读取切分
    response = json.loads(response)
    response = json.loads(response)
    # 数据库的存放列表
    databases_list = []
    # 将json数据中的数据库值提取出来放进去
    for databases in response['databases']:
        # 数据库名，拥有表数量
        database_info = {
            'database_name': databases['database_name'],
            'table_number': databases['table_number']
        }
        databases_list.append(database_info)
    return databases_list


# 获取表
def get_table(database, current_page):
    # 添加try容错
    try:
        # 获取表api接口请求
        request = ListAllTablesRequest()
        # 重定向获取条件
        request.database_name = database
        request.current_page = current_page
        request.page_size = 100
        # 获取表api接口
        response = client.list_all_tables(request)
        # 设置获取形式并转成json数据
        response = json.dumps(response, default=str, ensure_ascii=False)
        # 对json数据双重读取切分
        response = json.loads(response)
        response = json.loads(response)
        # 将表数据提取表名存放到列表中
        for table in response['tables']:
            table_name = table['table_name']
            table_list.append(table_name)
    except exceptions.ClientRequestException as e:
        print(e.status_code)
        print(e.error_msg)
    return table_list


# 获取字段
def get_columns(database, table):
    # 添加try容错
    try:
        # 获取表元信息api接口请求
        request = ShowDescribeTableRequest()
        # 重定向获取条件
        request.database_name = database
        request.table_name = table
        # 获取元信息api接口
        response = client.show_describe_table(request)
        # 设置获取形式并转成json数据
        response = json.dumps(response, default=str, ensure_ascii=False)
        # 对json数据双重读取切分
        response = json.loads(response)
        response = json.loads(response)
        # 获取当前时间
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # 将字段数据提取存放到列表
        for column in response['columns']:
            column_info = {
                'database_name': database,
                'table_name': table,
                'column_name': column['column_name'],
                'type': column['type'],
                'description': column['description'],
                'is_partition_column': column['is_partition_column'],
                'update_time': update_time
            }
            database_table_columns_list.append(column_info)
    except exceptions.ClientRequestException as e:
        print(e.status_code)
        print(e.error_msg)
    return database_table_columns_list


# 清空表数据
def truncate_data():
    pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq", timeout=20)
    sql1 = "truncate table data_center.all_tables_info"
    result1 = pymysql.update(sql1)


# 主函数，对表数据进行修改
def main(data_sum):
    pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq", timeout=20)
    for s in range(len(data_sum)):
        database_name = str(data_sum.iloc[s, 0])
        table_name = str(data_sum.iloc[s, 1])
        column_name = str(data_sum.iloc[s, 2])
        type = str(data_sum.iloc[s, 3])
        description = str(data_sum.iloc[s, 4])
        description = str.replace(description, "'", "_")
        is_partition_column = str(data_sum.iloc[s, 5])
        update_time = str(data_sum.iloc[s, 6])
        sql = "REPLACE INTO data_center.all_tables_info(database_name,table_name,column_name,type,description,is_partition_column,update_time) VALUES ('{}','{}','{}','{}','{}',{},'{}')".format(
            database_name, table_name, column_name, type, description, is_partition_column, update_time)
        # 对入库添加try容错
        try:
            result = pymysql.update(sql)
        except Exception as e:
            print(f"入库发生错误：{str(e)}")


if __name__ == '__main__':
    client = create_client()
    databases_list = get_databases()
    database_table_columns_list = []
    sum_list = []
    # 将数据进行生成汇总，分页获取api
    for database_info in databases_list:
        database_name = database_info['database_name']
        table_number = database_info['table_number']
        page_cnt = math.ceil(table_number / 100)
        table_list = []
        for current_page in range(page_cnt):
            current_page = current_page + 1
            table_list = get_table(database_name, current_page)
        for table in table_list:
            database_table_columns_list = get_columns(database_name, table)
            sum_list.append(database_table_columns_list)
            time.sleep(0.05)
    # 存储到dataframe格式数据容器
    data_sum = pd.DataFrame(database_table_columns_list)
    # 清除表数据
    truncate_data()
    # 修改表数据
    main(data_sum)
