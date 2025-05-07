# coding=utf-8
from dli.dli_client import DliClient
from dli.exception import DliException
from yssdk.common.printter import Printter
from yssdk.common.config import Config


class YsDliCLient:

    def __init__(self, conf_mode = 'prod'):

        conf = Config.get_config(conf_mode)
        auth_mode = conf['dli_conf']['ys_auth_mode']
        region = conf['dli_conf']['ys_region']
        project_id = conf['dli_conf']['ys_project_id']
        ak = conf['dli_conf']['ys_ak']
        sk = conf['dli_conf']['ys_sk']
        self.client = DliClient(auth_mode=auth_mode, region=region, project_id=project_id, ak=ak, sk=sk)

    def get_dli_client(self):
        return self.client

    def exec_sql(self, sql, quene):
        self.client.execute_sql(sql, db_name="yishou_data", queue_name=quene)

    def get_table_schema(self, db_name, tbl_name):
        try:
            table_info = self.client.get_table_schema(db_name, tbl_name)
            return table_info
        except DliException as e:
            Printter.error(e)
            return

if __name__ == '__main__':
    pass
