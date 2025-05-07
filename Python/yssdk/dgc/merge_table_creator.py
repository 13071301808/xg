# coding=utf-8
import string
from yssdk.obs.client import YsObsClient
from yssdk.mysql.client import YsMysqlClient
from yssdk.common.config import Config
from yssdk.constant.config import conf_dir
from dli.dli_client import DliClient
from dli.exception import DliException
import re
import traceback
import sys
import configparser
from yssdk.common.printter import Printter
import os


class YsDliMergeTableCreator:
    """
    用于生成merge表的建表语句， 创建merge表
    """
    def __init__(self, conf_mode='prod'):
        self.__conf_mode = conf_mode
        self.__conf = Config.get_config(conf_mode)

        auth_mode = self.__conf['dli_conf']['ys_auth_mode']
        region = self.__conf['dli_conf']['ys_region']
        project_id = self.__conf['dli_conf']['ys_project_id']
        ak = self.__conf['dli_conf']['ys_ak']
        sk = self.__conf['dli_conf']['ys_sk']


        self.obs_client = YsObsClient(conf_mode = conf_mode)
        self.dli_client = DliClient(auth_mode=auth_mode, region=region, project_id=project_id, ak=ak, sk=sk)

    def change_type(self, cloumns_type):
        '''
        # r7表 => merge表类型转换
        '''
        if re.search(r'int', cloumns_type) != None:
            return 'BIGINT'
        elif re.search(r'decimal', cloumns_type) != None:
            return 'DOUBLE'
        elif re.search(r'varchar', cloumns_type) != None:
            return 'STRING'
        elif re.search(r'timestamp', cloumns_type) != None:
            return 'STRING'
        elif re.search(r'text', cloumns_type) != None:
            return 'STRING'
        elif re.search(r'datetime', cloumns_type) != None:
            return 'STRING'
        else:
            return cloumns_type

    def get_rconnect(self):
        '''
        连接r7数据库【mysql】
        :return: mysql_client
        '''
        mysql_client = YsMysqlClient(conf_mode = self.__conf_mode)
        mysql_client.get_r7connect()
        return mysql_client

    def get_zzconnect(self):
        '''
        连接自主取数数据库【mysql】
        :return: mysql_client
        '''
        mysql_client = YsMysqlClient(conf_mode = self.__conf_mode)
        mysql_client.get_zzconnect()
        return mysql_client

    def get_r7_create_table(self, r7_table_name, dli_table_name, dli_db='yishou_data', bucket_name = 'yishou-bigdata'):
        """
        拿到r7的表结构生成merge建表语句
        :param r7_table_name: 业务表的名字,包含库名
        :param dli_table_name: DLI表的名字
        :param dli_db:DLI的库名
        :param bucket_name: 华为云OBS bucket名字
        :return: merge建表语句, string类型
        """
        mysql_client = self.get_rconnect()
        sql = 'SHOW FULL COLUMNS FROM ' + str(r7_table_name)
        body_sql = ''
        template = "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' \nSTORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'\n LOCATION '${location}' \nTBLPROPERTIES ('orc.compress' = 'SNAPPY' );"

        template_bottom_sql = string.Template(template)

        try:
            create_table_tuple = mysql_client.select(sql)
            for i in range(len(create_table_tuple)):
                cloumns_type = self.change_type(str(create_table_tuple[i][1]))
                if i < (len(create_table_tuple) - 1):
                    body_sql = body_sql + '  ' + str(
                        create_table_tuple[i][0]) + ' ' + cloumns_type + ' comment \"' + str(
                        create_table_tuple[i][8]) + '\",\n'
                else:
                    body_sql = body_sql + '  ' + str(
                        create_table_tuple[i][0]) + ' ' + cloumns_type + ' comment \"' + str(
                        create_table_tuple[i][8]) + '\",\n' + 'nano_time STRING COMMENT "binlog日期")\n '
            header_sql = 'create table if not exists %s.all_%s_merge(\n' % (dli_db, dli_table_name)
            location = 'obs://%s/%s.db/all_%s_merge' % (bucket_name, dli_db, dli_table_name)
            bottom_sql = template_bottom_sql.safe_substitute(location=location)
            create_sql = header_sql + body_sql + bottom_sql
            return (create_sql)
        except Exception as e:
            sys.exit(traceback.format_exc())

    def create_table(self, r7_table_name, dli_db='yishou_data', queue = 'develop', temp = True, bucket_name = 'yishou-bigdata'):
        """
        创建DLI外部表
        :param r7_table_name: 业务表的名字,包含库名
        :param dli_db: DLI的库名
        :param queue: DLI的队列名
        :param temp: 是否创建temp表，在表名后面添加_temp后缀, Ture|False,  boolean类型
        :param bucket_name: 华为云OBS bucket名字
        :return: None
        """
        dli_table_name = re.findall('\.(.*)',r7_table_name)[0]
        create_sql= self.get_r7_create_table(r7_table_name, dli_table_name)
        create_temp_sql = 'create table if not exists %s.all_%s_temp like %s.all_%s'%(dli_db, dli_table_name, dli_db, dli_table_name)
        obs_dir = '%s.db/%s'%(dli_db, dli_table_name)
        try:
            Printter.info(create_sql)
            self.obs_client.create_obs_dir(bucket_name, obs_dir)
            self.dli_client.execute_sql(create_sql, dli_db, queue)
            if temp == True:
                self.dli_client.execute_sql(create_temp_sql, dli_db, queue)
            Printter.info("执行成功")
        except DliException as e:
            Printter.error(traceback.format_exc())
            sys.exit('建表失败')