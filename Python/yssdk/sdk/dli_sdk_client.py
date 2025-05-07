# -*-coding: Utf-8 -*-
# @File : sdk_client .py
# author: ShanFeng
# Time：2024/3/5/0005

import json
import re
from huaweicloudsdkcore.exceptions.exceptions import ClientRequestException
from huaweicloudsdkdgc.v1.region.dgc_region import DgcRegion
from concurrent.futures.thread import ThreadPoolExecutor
from huaweicloudsdkdgc.v1 import *
from queue import Queue
from requests.exceptions import RetryError, HTTPError
from yssdk.sdk.base import SDK_Client


class DLI_SDK_Client(SDK_Client):
    """
    集成了华为云DLI各API的小工具
    """
    def __init__(self):
        super().__init__()
        credentials = super().get_credentials()
        region = super().get_region()

        self.client = DgcClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(DgcRegion.value_of(region)) \
            .build()
        self.queue = Queue()
        self.workspaces = {"yishou_data": "7ac283973592445f81af536d903d95f4",
                           "yishou_daily": "429bc7103cd04b0c97d69038cd67f872",
                           "yishou_apex": "4cf84d1344b941ea808a72d1e4fc53a9"}
        self.listApproversbody = [
            JobApprover(
                approver_name="yangshibiao"
            ),
            JobApprover(
                approver_name="zouyi"
            ),
            JobApprover(
                approver_name="hw14505396"
            )
        ]

    def show_job(self, job_name, workspace_name=''):
        """
        传入作业名, 返回json格式作业信息
        :param workspace_name: 作业空间{yishou_data, yishou_daily, yishou_apex}
        :param job_name:作业名
        :return: json
        """
        request = ShowJobRequest()
        request.job_name = job_name
        workspace_id = self.workspaces.get(workspace_name)

        if workspace_id is not None:
            try:
                request.workspace = workspace_id
                response = self.client.show_job(request)
                return json.loads(str(response))
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return job_name, workspace_name

        for workspace_id in self.workspaces.values():
            request.workspace = workspace_id
            try:
                response = self.client.show_job(request)
                return json.loads(str(response))
            except ClientRequestException:
                continue
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return job_name, workspace_name

        return None

    def show_job_status(self, job_name, workspace_name=''):
        """
        传入作业名, 查看作业当前调度状态
        :param workspace_name: 作业空间{yishou_data, yishou_daily, yishou_apex}
        :param job_name: 作业名
        :return: json
        """
        request = ShowJobStatusRequest()
        request.job_name = job_name
        workspace_id = self.workspaces.get(workspace_name)
        if workspace_id is not None:
            try:
                request.workspace = workspace_id
                response = self.client.show_job_status(request)
                return json.loads(str(response))
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return job_name, workspace_name

        for workspace_id in self.workspaces.values():
            request.workspace = workspace_id
            try:
                response = self.client.show_job_status(request)
                return json.loads(str(response))
            except ClientRequestException:
                continue
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return job_name, workspace_name

        return None

    def delete_job(self, job_name, workspace_name):
        """
        谨慎使用!!!传入作业名, 作业空间, 删除作业
        :param workspace_name: 作业空间{yishou_data, yishou_daily, yishou_apex}
        :param job_name: 作业名
        :return: json
        """
        request = ShowJobStatusRequest()
        request.job_name = job_name
        request.workspace = self.workspaces.get(workspace_name)
        response = self.client.delete_job(request)
        return response

    def show_job_dependencies(self, job_name, job_stream="当前作业"):
        """
        传入作业名, 查询作业的调度状态
        :param job_name:
        :param job_stream:
        :return: None
        """
        # 查询当前作业的调度表达式
        content = self.show_job(job_name)
        status = self.show_job_status(job_name)
        print(job_stream, content['schedule']['cron']['expression'], status['status'], job_name)

    def show_all_job_dependencies(self, job_name, downstream_jobs, thread_num=1):
        """
        传入作业名与下游依赖列表, 查看依赖时间与作业调度状态
        :param thread_num: 线程数, 默认1
        :param job_name: 作业名
        :param downstream_jobs: 下游作业列表
        :return: None
        """
        pool = ThreadPoolExecutor(max_workers=thread_num)
        # 查询当前作业的调度表达式
        self.show_job_dependencies(job_name)

        # 查询当前作业的上游依赖作业
        content = self.show_job(job_name)
        depend_jobs = content['schedule']['cron']['dependJobs']['jobs']
        for job in depend_jobs:
            self.queue.put(job)
        for i in range(self.queue.qsize()):
            arg_tuple = (self.queue.get(), "上游作业")
            pool.submit(self.show_job_dependencies, *arg_tuple)

        # 查询当前作业的下游依赖作业
        for job in downstream_jobs:
            self.queue.put(job)
        for i in range(self.queue.qsize()):
            arg_tuple = (self.queue.get(), "下游作业")
            pool.submit(self.show_job_dependencies, *arg_tuple)

        # 等待所有线程完成
        pool.shutdown()

    def show_file_info(self, path_name):
        """
        查询作业文件
        :param path_name: 路径名
        :return: json
        """
        request = ShowFileInfoRequest()
        request.body = FilePath(path_name)
        for workspace in self.workspaces.values():
            request.workspace = workspace
            try:
                response = self.client.show_file_info(request)
                return json.loads(str(response))
            except ClientRequestException:
                continue
        return None

    def list_jobs(self, path='', workspace_name='', limit=10, offset=0):
        """
        获取目录下作业
        :param path:一级目录
        :param workspace_name: 作业空间{yishou_data, yishou_daily, yishou_apex}
        :param limit: 输出数
        :param offset: 偏移量
        :return: json
        """
        request = ListJobsRequest()
        request.limit = limit
        request.offset = offset
        request.job_type = "BATCH"
        request.job_name = path
        workspace_id = self.workspaces.get(workspace_name)
        if workspace_id is not None:
            try:
                request.workspace = workspace_id
                response = self.client.list_jobs(request)
                return json.loads(str(response))
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[1:-1])
                return path, workspace_name

        for workspace_id in self.workspaces.values():
            request.workspace = workspace_id
            try:
                response = self.client.list_jobs(request)
                return json.loads(str(response))
            except ClientRequestException:
                continue
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return path, workspace_name
        return None

    def show_script(self, script_name, workspace_name=''):
        """
        传入脚本名, 返回json格式脚本信息
        :param workspace_name: 作业空间{yishou_data, yishou_daily, yishou_apex}
        :param script_name:作业名
        :return: json
        """
        request = ShowScriptRequest()
        request.script_name = script_name
        workspace_id = self.workspaces.get(workspace_name)

        if workspace_id is not None:
            try:
                request.workspace = workspace_id
                response = self.client.show_script(request)
                return json.loads(str(response))
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return script_name, workspace_name
            except ClientRequestException as c:
                self.printter.warn(f"The script does not exist: {workspace_name}, {script_name} ")
                return None

        for workspace_id in self.workspaces.values():
            request.workspace = workspace_id
            try:
                response = self.client.show_script(request)
                return json.loads(str(response))
            except ClientRequestException as c:
                continue
            except RetryError as e:
                caused_by_message = str(e).split("ResponseError")[-1]
                self.printter.warn("ResponseError", caused_by_message[:-1])
                return script_name, workspace_name

    def update_script(self, script_name, workspace_name, old_data, new_data):
        script_data = self.show_script(script_name, workspace_name)
        pattern = r'\b{}\b'.format(re.escape(old_data))
        listApproversbody = self.listApproversbody

        # 使用 re 模块进行搜索
        count = None
        if script_data:
            count = len(re.findall(pattern, script_data.get('content', '')))

        if count:
            try:
                request = UpdateScriptRequest()
                request.workspace = self.workspaces[workspace_name]
                request.script_name = script_name

                request.body = ScriptInfo(
                    target_status="SUBMITTED",
                    description=script_data.get('description'),
                    configuration=json.loads(script_data.get('configuration').replace("'", "\"")),
                    queue_name=script_data.get('queueName'),
                    database=script_data.get('database'),
                    connection_name=script_data.get('connectionName', 'DLI'),
                    content=re.sub(pattern, new_data, script_data.get('content')),
                    directory=script_data.get('directory'),
                    type=script_data.get('type'),
                    name=script_data.get('name'),
                    approvers=listApproversbody

                )
                self.client.update_script(request)
                self.printter.info(
                    f"Update script content success! Workspace:{workspace_name}, Script:{script_name}, Substitution number:{count}")
                return count
            except ClientRequestException as e:
                self.printter.error(
                    f"Update script content failed! Workspace:{workspace_name}, Script:{script_name}, Error_msg:{e.error_msg}")
        elif count == 0:
            self.printter.warn(
                f"Update script content failed! Workspace:{workspace_name}, Script:{script_name}, Error_msg:Old data does not exist in the script content")
