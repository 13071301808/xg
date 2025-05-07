# coding=utf-8
import sys
from yssdk.dli.sql_client import YsDliSQLCLient
from yssdk.gaussdb.client import YsGaussDBClient
from yssdk.dgc.client import YsDgcClient
from yssdk.common.config import Config
from yssdk.common.printter import Printter
import string
import pandas as pd
import queue
import threading
import time


class HistoryRunnerThread(threading.Thread):
    def __init__(self, thread_id, thread_name, date_queue, sql_template, db_type='dli', conf_mode='prod'):
        """
        初始化方法
        :param thread_id: 线程id
        :param thread_name: 线程名
        :param date_queue:  日期队列
        :param sql_template: SQL模板
        :param db_type:  数据库类型, dli|dws, 默认值：dli
        :param conf_mode:  配置文件的环境, dev|prod, 默认值：prod, 本地开发调试使用dev
        """
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.thread_name = thread_name
        self.date_queue = date_queue
        self.db_type = db_type
        self.conf_mode = conf_mode
        self.sql_template = sql_template
        self.exit_flag = 0
        if self.db_type == 'dli':
            self.client = YsDliSQLCLient(self.conf_mode)
            self.handler = self.run_dli
        elif self.db_type == 'dws':
            self.client = YsGaussDBClient(self.conf_mode)
            self.handler = self.run_dws
        else:
            sys.exit("未知数据库类型")

    def run(self):
        Printter.info("开启线程：" + self.name)
        self.process_data(self.handler)
        Printter.info("退出线程：" + self.name)

    def process_data(self, handler):
        """
        执行
        :param handler: 执行函数： run_dli | run_dws
        :return:
        """
        while not self.exit_flag:
            if not self.date_queue.empty():
                run_date = self.date_queue.get()
                bizdate = run_date.strftime("%Y%m%d")
                Printter.info("%s processing %s" % (self.thread_name, bizdate))
                handler(bizdate)
            else:
                continue
            time.sleep(1)

    def run_dli(self, bizdate):
        """
        执行 dli sql
        :param bizdate: 补数日期
        :return:
        """
        sql = self.sql_template.safe_substitute(bizdate=bizdate)
        self.client.exec_multi_sql(sql)

    def run_dws(self, bizdate):
        """
        执行 dws sql
        :param bizdate: 补数日期
        :return:
        """
        sql = self.sql_template.safe_substitute(bizdate = bizdate, cyctime = int(bizdate) + 1)
        self.client.update(sql)

    def set_exit_flag(self):
        """
        通知线程退出, self.exit_flag = 1 时退出
        :return:
        """
        self.exit_flag = 1


class HistoryRunner:
    def __init__(self, thread_num, start_date, end_date, workspace_name, script_name, db_type='dli', conf_mode='prod'):
        """
        初始化方法
        :param thread_num: 并发线程数
        :param start_date: 补数开始时间
        :param end_date: 补数结束时间
        :param workspace_name: 工作空间名
        :param script_name: 脚本名
        :param db_type: 数据库类型, dli|dws, 默认值：dli
        :param conf_mode: 配置文件的环境, dev|prod, 默认值：prod, 本地开发调试使用dev
        """
        if thread_num > 10:
            sys.exit('程序退出，线程数过高可能会影响集群其它作业的运行，建议线程数为：5， 最大不超过10')
        self.thread_num = thread_num
        self.start_date = start_date
        self.end_date = end_date
        self.workspace_name = workspace_name
        self.script_name = script_name
        self.conf_mode = conf_mode
        self.db_type = db_type

        # 读取配置文件
        self.__conf_mode = conf_mode
        self.__conf = Config.get_config(conf_mode)
        self.__base_url = self.__conf['dgc_conf']['base_url']
        self.__project_id = self.__conf['dli_conf']['ys_project_id']

    def rerun(self):
        """
        执行补数据
        :return:
        """
        date_list = pd.date_range(start=self.start_date, end=self.end_date, freq="D")
        work_queue = queue.Queue(len(date_list))
        client = YsDgcClient(self.conf_mode)
        url = '%s/%s/scripts/%s' % (self.__base_url, self.__project_id, self.script_name)
        content = client.get_script_content(method='GET',
                                            worspace_name=self.workspace_name,
                                            url=url)
        content = content.replace('bdp.system.bizdate', 'bizdate').replace('bdp.system.cyctime', 'cyctime')
        sql_template = string.Template(content)

        threads = []
        for thread_id in range(1, self.thread_num + 1):
            thread_name = 'Thread-%d' % thread_id
            thread = HistoryRunnerThread(thread_id, thread_name, work_queue,
                                         sql_template=sql_template,
                                         db_type=self.db_type,
                                         conf_mode=self.conf_mode)
            thread.start()
            threads.append(thread)

        # 填充队列
        for word in date_list:
            work_queue.put(word)

        # 等待队列清空
        while not work_queue.empty():
            pass

        # 通知线程是时候退出
        for thread in threads:
            thread.set_exit_flag()

        # 等待所有线程完成
        for t in threads:
            t.join()
        Printter.info("退出主线程")
