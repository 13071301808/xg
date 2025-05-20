import subprocess
import shlex
import os
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from huaweicloudsdkdli.v1.region.dli_region import DliRegion
from huaweicloudsdkcore.exceptions import exceptions
from huaweicloudsdkdli.v1 import *

class HuaweiCloudClient:
    def __init__(self):
        self.ak = os.getenv("HUAWEI_CLOUD_AK", "BOYRQSQ8XUPWJLLKZZSO")
        self.sk = os.getenv("HUAWEI_CLOUD_SK", "1zeUjOpGgXldkdsTQZLiFi48lGkR9V0ort65q1oy")
        self.credentials = BasicCredentials(self.ak, self.sk)
        self.workspaces = {
            "yishou_data": "7ac283973592445f81af536d903d95f4",
            "yishou_daily": "429bc7103cd04b0c97d69038cd67f872",
            "yishou_apex": "4cf84d1344b941ea808a72d1e4fc53a9"
        }
        self.dli_client = DliClient.new_builder() \
            .with_credentials(self.credentials) \
            .with_region(DliRegion.value_of("cn-south-1")) \
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

    def run_linux_command(self, database_name, table_name):
        # 复杂命令
        command = f'E:/obsutil_windows_amd64_5.5.12/obsutil ls obs://yishou-bigdata/{database_name}.db/{table_name}/ -du -limit=0'
        # 使用 shlex.split 拆分命令字符串
        args = shlex.split(command)
        # 运行命令
        result = subprocess.run(args, capture_output=True, text=True)
        # 打印命令的输出
        for line in result.stdout.splitlines():
            if "Total prefix" in line:
                size_info = line.split("size:")[1].strip()
                print(size_info)


# 使用示例
if __name__ == "__main__":
    database_name = 'yishou_data'
    table_name = 'all_fmys_cart_detail_h'
    client = HuaweiCloudClient()
    client.run_linux_command(database_name, table_name)