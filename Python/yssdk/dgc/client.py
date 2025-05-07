# coding=utf-8
import string
from yssdk.apig import signer
from yssdk.common.printter import Printter
from yssdk.constant.template import template
from yssdk.dgc.merge_table_creator import YsDliMergeTableCreator
from yssdk.common.config import Config
from yssdk.mysql.client import YsMysqlClient
import requests
import os
import json


class YsDgcClient:
    """
    Dgc客户端，目前可支持创建华为云DGC作业
    """
    def __init__(self, conf_mode='prod'):
        conf = Config.get_config(conf_mode)
        yssdk_code_dir = os.path.abspath(os.path.join(os.path.dirname("__file__"),os.path.pardir))
        if conf_mode == 'dev':
            self.__template_dir = yssdk_code_dir + '/template'
        elif conf_mode == 'prod':
            self.__template_dir = conf['template']['template_dir']

        self.__sig = signer.Signer()
        self.__sig.Key = conf['dli_conf']['ys_ak']
        self.__sig.Secret = conf['dli_conf']['ys_sk']

        self.url = conf['dgc_conf']['job_url']

        self.__default_body = ''

        self.mysql_client = YsMysqlClient(conf_mode = conf_mode)
        self.mysql_client.get_zzconnect()

    def init_request(self, method, url):
        """
        初始化HTTP请求
        :param method: HTTP请求类型, GET,POST等, string类型
        :param url: HTTP请求URL, string类型
        :return: YsDgcClient 对象实例
        """
        self.request = signer.HttpRequest(method=method, url=url)
        self.request.headers = {"content-type": "application/json;charset=UTF-8"}
        return self

    def add_request_header(self, header):
        """
        添加HTTP请求的header
        :param header: 字典类型
        :return: YsDgcClient 对象实例
        """
        if len(header) > 0:
            current_header = self.request.headers
            new_header = current_header.copy()
            new_header.update(header)
            self.request.headers = new_header
        return self

    def set_workspace(self, workspace_name):
        """
        在http header添加workspace_id
        :param workspace_name:华为云DGC工作空间名字,如：yishou_data, yishou_daily, yishou_apex等
        :return: YsDgcClient 对象实例
        """
        workspace_id = self.get_workspace_from_db(workspace_name)
        header = {"workspace": workspace_id}
        self.add_request_header(header)
        return self

    def get_workspace_from_db(self, workspace_name):
        """
        从数据库获取华为云DGC的workspace_id
        :param workspace_name: 华为云DGC的空间名,如：yishou_data, yishou_daily, yishou_apex等
        :return: workspace_id, string类型
        """
        sql = "select workspace from ys_dgc.dgc_workspace where workspace_name = '%s'"%(workspace_name)
        return self.mysql_client.select(sql)[0][0]

    def set_request_body(self, body):
        """
        设置http请求的body
        :param body: string类型
        :return:YsDgcClient 对象实例
        """
        if len(body) > 0:
            self.request.body = body
        return self

    def send_request(self):
        """
        发送http请求到华为云DGC接口
        :return: http response 对象
        """
        self.__sig.Sign(self.request)
        resp = requests.request(self.request.method, self.request.scheme + "://" + self.request.host + self.request.uri,
                                headers=self.request.headers, data=self.request.body)
        return resp

    def create_job(self, method, url, workspace_name, header = {}, body = ''):
        """
        创建华为DGC作业
        :param method: POST
        :param url: HTTP请求URL, string类型
        :param workspace_name: 华为云DGC工作空间名,如：yishou_data, yishou_daily, yishou_apex等
        :param header: 字典类型
        :param body: string类型
        :return: Response对象
        """
        resp = self.init_request(method, url)\
            .set_workspace(workspace_name)\
            .set_request_body(body)\
            .add_request_header(header)\
            .send_request()
        return resp


    def create_script(self, method, url, workspace_name, header = {}, body = ''):
        """
        创建华为DGC脚本
        :param method: POST
        :param url: HTTP请求URL, string类型
        :param workspace_name: 华为云DGC工作空间名,如：yishou_data, yishou_daily, yishou_apex等
        :param header: 字典类型
        :param body: string类型
        :return: Response对象
        """
        resp = self.init_request(method, url) \
            .set_workspace(workspace_name) \
            .set_request_body(body) \
            .add_request_header(header) \
            .send_request()
        return resp

    def update_binlog_configure_center(self, table_name, primary_column, quene='develop'):
        """
        在配置中心插入新的记录
        :param table_name: 业务表名
        :param primary_column: 主键字段，联合主键用逗号分隔
        :param quene: DLI 队列名
        :return: None
        """
        insert_template = "insert into data_center.register_schema set \
                topic ='bigdata_mysql_binlog_avro',\
                business_table_name ='${table_name}',\
                primary_column ='${primary_column}',\
                binlog_type ='2',\
                create_time = now(),\
                quene ='${quene}'"

        insert_template_sql = string.Template(insert_template)
        insert_sql = insert_template_sql.safe_substitute(table_name=table_name,
                                                         primary_column=primary_column,
                                                         quene=quene)
        check_sql = "select * from data_center.register_schema where business_table_name ='%s'" % table_name
        Printter.info('在配置中心新增表：%s' % table_name)
        if len(self.mysql_client.select(check_sql, None)) == 0:
            self.mysql_client.update(sql=insert_sql)
        else:
            Printter.info('配置已存在，跳过')

    def generate_dgc_job_setting(self, parameters, action_type=None, time_type=None, template_file=None):
        """
        生成dgc作业的配置，JSON格式
        :param parameters: 作业的参数配置,字典类型
        :param action_type: 执行类型,merge|insert
        :param time_type: 表调度的时间类型, day|hour
        :param template_file: 模板文件的路径
        :return: gc作业的配置，JSON格式字符串
        """
        if template_file is None:
            template_file = template[action_type][time_type]
        file = open(self.__template_dir +'/' + template_file, encoding='utf-8')
        content = file.read()
        template_setting = string.Template(content)
        setting = template_setting.safe_substitute(parameters)
        return setting

    def get_script_content(self, method, workspace_name, url):
        """
        获取脚本内容
        :param method:
        :param workspace_name:
        :param url:
        :return:
        """
        resp = self.init_request(method, url) \
            .set_workspace(workspace_name) \
            .send_request()
        content = json.loads(resp.text)
        return content['content']

    def check_job_exists(self, method, workspace_name, url):
        """
        查看作业是否存在
        :param method:
        :param workspace_name:
        :param url:
        :return:
        """
        job_exists = False
        resp = self.init_request(method, url) \
            .set_workspace(workspace_name) \
            .send_request()
        content = json.loads(resp.text)
        num = int(content['total'])
        if num > 0:
            job_exists = True
        return job_exists

    def get_job_setting(self,workspace_name, job_name):
        """
        获取作业的配置信息，以json格式返回
        :param workspace_name: 工作空间, 如:yishou_data, yishou_daily
        :param job_name: DGC作业名
        :return:
        """
        method = 'GET'
        job_url = self.url + '/' + job_name
        resp = self.init_request(method, job_url) \
            .set_workspace(workspace_name) \
            .send_request()
        content = json.loads(resp.text)
        return content

    def run_immediate_job(self, workspace_name, job_name):
        """
        运行DGC作业，相当于DGC作业开发的测试运行
        :param workspace_name: 工作空间, 如:yishou_data, yishou_daily
        :param job_name: DGC作业名
        :return:
        """
        method = 'POST'
        start_job_url = self.url + '/' + job_name + '/run-immediate'
        resp = self.init_request(method, start_job_url) \
            .set_workspace(workspace_name) \
            .send_request()
        if resp.status_code == 200:
            Printter.info(job_name+' summbit successfully')
        else:
            Printter.error(job_name+' summbit with failure')





if __name__ == '__main__':
    pass

