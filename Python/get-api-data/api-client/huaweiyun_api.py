# 创建华为云api连接
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkdli.v1.region.dli_region import DliRegion
from huaweicloudsdkdli.v1 import *
from huaweicloudsdkcore.exceptions import exceptions


def huaweiyun_token():
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


# 建立连接华为云api


client = huaweiyun_token()


class HuaWeiYun:
    # 查询所有库
    @staticmethod
    def select_databases():
        try:
            request = ListDatabasesRequest()
            response_db = client.list_databases(request)
            return response_db
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    # 检测库中表是否存在
    @staticmethod
    def check_table(db_name, table_name):
        try:
            request_ct = ShowDescribeTableRequest()
            request_ct.database_name = db_name
            request_ct.table_name = table_name
            response_ct = client.show_describe_table(request_ct)
            return response_ct
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    # 查询所有表
    @staticmethod
    def select_tables():
        try:
            request_tb = ListAllTablesRequest()
            response_tb = client.list_all_tables(request_tb)
            return response_tb
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    # 查询sql作业
    @staticmethod
    def select_sql_job():
        try:
            request_sj = ListSqlJobsRequest()
            response_sj = client.list_sql_jobs(request_sj)
            return response_sj
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)

    # 取消sql作业
    @staticmethod
    def cancel_sql_job(job_id):
        try:
            request_csj = CancelSqlJobRequest()
            request_csj.job_id = job_id
            response_csj = client.cancel_sql_job(request_csj)
            return response_csj
        except exceptions.ClientRequestException as e:
            print(e.status_code)
            print(e.request_id)
            print(e.error_code)
            print(e.error_msg)
