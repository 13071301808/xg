## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2024/07/23 20:23:19 GMT+08:00
## 功能：获取华为云所有库表信息，包括库名、表名、存放具体位置、表内存
## ******************************************************************** ##
import threading
import pymysql
import math
import subprocess
import shlex
import os
from datetime import datetime
from huaweicloudsdkcore.http.http_config import HttpConfig
from huaweicloudsdkdli.v1.region.dli_region import DliRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkdli.v1 import DliClient, ListTablesRequest
from yssdk.obs.client import YsObsClient
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkdataartsstudio.v1 import *
from loguru import logger
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

# 全局配置
# 不处理表
# not_deal = ['dim_cat_price_period_info', 'dim_storage_work_order_category_info',
#             'dim_cleaning_route_home_big_index_info', 'dim_calendar', 'dim_province_area']
MAX_RUNTIME = 10800  # 最大运行时间（秒）
# 配置限流策略
config = HttpConfig.get_default_config()
config.timeout = 60  # 设置请求超时时间为 60 秒


class MysqlClient:
    def __init__(self, db, host, user, password):
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset="utf8mb4")
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.close()

    def select(self, sql, args):
        self.cursor.execute(query=sql, args=args)
        return self.cursor.fetchall()

    def get_cursor(self):
        return self.cursor

    def get_conn(self):
        return self.conn

    # 删除、更新、插入
    def update(self, sql):
        temp = self.cursor.execute(sql)
        self.conn.commit()
        return temp

    def update2(self, sql, params):
        temp = self.cursor.execute(sql, params)
        self.conn.commit()
        return temp

    def close(self):
        self.cursor.close()
        self.conn.close()


class HuaweiCloudClient:
    def __init__(self):
        self.ak = os.getenv("HUAWEI_CLOUD_AK", "EULIXLKJQLUS62GPNIAX")
        self.sk = os.getenv("HUAWEI_CLOUD_SK", "vmoTDJVUOXVtwt8pwh5LQ6LnE2bJPDtxid7biimK")
        self.credentials = BasicCredentials(self.ak, self.sk)
        self.workspaces = {
            "yishou_data": "7ac283973592445f81af536d903d95f4",
            "yishou_daily": "429bc7103cd04b0c97d69038cd67f872",
            "yishou_apex": "4cf84d1344b941ea808a72d1e4fc53a9"
        }
        self.dli_client = DliClient.new_builder() \
            .with_credentials(self.credentials) \
            .with_region(DliRegion.value_of("cn-south-1")) \
            .with_http_config(config) \
            .build()

    # 获取所有的数据库
    def fetch_all_database_info(self):
        try:
            request = ListDatabasesRequest()
            request.with_priv = True
            return self.dli_client.list_databases(request)
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    # 获取所有表的具体信息
    def fetch_table_info(self, database_name, current_page, size):
        try:
            request = ListTablesRequest()
            request.database_name = database_name
            request.current_page = current_page
            request.page_size = size
            request.with_detail = True
            resp = self.dli_client.list_tables(request)
            return resp
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    def run_linux_command(self, database_name, table_name):
        # 复杂命令
        command = f'/apps/utils/obsutil_linux_amd64_5.5.12/obsutil ls obs://yishou-bigdata/{database_name}.db/{table_name}/ -du -limit=0'
        # 使用 shlex.split 拆分命令字符串
        args = shlex.split(command)
        # 初始化size_info
        size_info = '获取失败需人为查询'
        # 运行命令
        result = subprocess.run(args, capture_output=True, text=True)
        # 打印命令的输出
        for line in result.stdout.splitlines():
            if "Total prefix" in line:
                size_info = line.split("size:")[1].strip()
                break  # 找到后立即退出循环
        return size_info


# pymysql = MysqlClient("fmdes", "121.37.27.174", "fmdes_user", "6IveUZJyx1ifeWGq")
pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq")
obs_client = YsObsClient()


# 转换内存单位
def tranfrom_size(table_size):
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(table_size)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    table_size_str = f"{size:.2f}{units[idx]}"
    return table_size_str


# 获取所有表信息到库中
def insert_table_info(db_data):
    database_name, current_page, size = db_data
    resp = huaweiyun_client.fetch_table_info(database_name, current_page, size)
    for tb in resp.tables:
        logger.info(f'{threading.current_thread().name}| 数据表：{tb.table_name}')
        # 插数模版
        insert_sql = "insert into data_center.dli_tables_size (database_name, table_name, table_type, location, table_size,updated_at) value(%s,%s,%s,%s,%s,%s)"
        if tb.data_location == 'OBS':
            try:
                # 走obs的api查看文件内存
                table_size_str = huaweiyun_client.run_linux_command(database_name, tb.table_name)
                # logger.info(f'{threading.current_thread().name}| 该表内存：{table_size_str}')
            except Exception as e:
                logger.info(f'{threading.current_thread().name}| 报错原因：{e}')
            with lock:
                updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                pymysql.update2(insert_sql,
                                (database_name, tb.table_name, tb.table_type, tb.location, table_size_str, updated_at))
        elif tb.data_location != 'OBS' or tb.data_location == '':
            # 转换内存单位
            table_size_str = tranfrom_size(tb.table_size)
            with lock:
                updated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                pymysql.update2(insert_sql, (database_name, tb.table_name, tb.location, table_size_str, updated_at))
        else:
            logger.info(f'{threading.current_thread().name}| 在白名单中或者其他情况')


def timeout_handler():
    print("Maximum runtime exceeded. Stopping program.")
    os._exit(0)  # 停止程序


if __name__ == '__main__':
    # 配置日志
    logger.add(f"/data/project/test/xiaogao/dli_table_{datetime.now().strftime('%Y-%m-%d')}.log", retention="1 days")
    # 配置队列及线程
    queue = Queue()
    pool = ThreadPoolExecutor(max_workers=12)
    lock = Lock()
    huaweiyun_client = HuaweiCloudClient()
    # 清空数据
    pymysql.update('truncate table data_center.dli_databases_xg')
    pymysql.update('truncate table data_center.dli_tables_size')
    logger.info(f'{threading.current_thread().name}| 清空表数据')
    # 获取所有的数据库
    dbs = huaweiyun_client.fetch_all_database_info()
    # 计算所有表数量
    table_num_list = [db_num.table_number for db_num in dbs.databases]
    table_sum = sum(table_num_list)
    logger.info(f'{threading.current_thread().name}| 表数量：{table_sum}')

    # # 设置超时处理
    timer = threading.Timer(MAX_RUNTIME, timeout_handler)
    timer.start()
    try:
        for db in dbs.databases:
            logger.info(f'{threading.current_thread().name}| 数据库：{db.database_name}')
            # 更新记录华为云的库信息
            pymysql.update(
                f"insert into data_center.dli_databases_xg (owner, description, database_name, enterprise_project_id, resource_id,table_number)"
                f"value('{db.owner}','{db.description}','{db.database_name}','{db.enterprise_project_id}','{db.resource_id}',{db.table_number})"
            )
            # 获取所有表的具体信息
            for index in range(1, math.ceil(db.table_number / 50) + 1):
                # 插入所有
                pool.submit(insert_table_info, [db.database_name, index, 50])
        # 结束线程
        pool.shutdown(wait=True)
    finally:
        timer.cancel()  # 取消定时器



