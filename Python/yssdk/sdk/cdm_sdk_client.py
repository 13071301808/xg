# -*-coding: Utf-8 -*-
# @File : cdm_sdk_client .py
# author: ShanFeng
# Time：2024/3/5/0005

from huaweicloudsdkcdm.v1.region.cdm_region import CdmRegion
from huaweicloudsdkcdm.v1 import *

from yssdk.sdk.base import SDK_Client


class CDM_SDK_Client(SDK_Client):
    """
    集成了华为云CDM各API的小工具
    """

    def __init__(self):
        super().__init__()
        credentials = super().get_credentials()
        region = super().get_region()

        self.client = CdmClient.new_builder() \
            .with_credentials(credentials) \
            .with_region(CdmRegion.value_of(region)) \
            .build()

    def create_cdm_job(self, mysql_db, mysql_table, columns, cluster_id=None, queue=None, from_link_name=None,
                       from_connector_name=None, to_link_name=None, to_connector_name=None, to_db=None, to_table=None,
                       shouldClearTable=None, cdm_job_name=None, insert_dt=None):
        """
        传入CDM相关配置参数, 一键生成CDM作业
        :param mysql_db: 业务库名
        :param mysql_table: 业务表名
        :param columns: 字段列表, 以"&"拼接的字符串
        :param cluster_id: 集群ID
        :param queue: 队列名
        :param from_link_name: 源端连接名
        :param from_connector_name: 源端连接器
        :param to_link_name: 接收端连接名
        :param to_connector_name: 接收端连接器
        :param to_db: 接收端库名
        :param to_table: 接收端表名
        :param shouldClearTable: 是否清空表格
        :param cdm_job_name: cdm_job名称
        :param insert_dt: 插入分区的base64编码
        """

        request = CreateJobRequest()

        if cluster_id is None:
            request.cluster_id = super().get_cluster_id('cdm_3058')
        if queue is None:
            queue = 'develop'
        if from_link_name is None:
            from_link_name = 'yishou_db_slave'
        if from_connector_name is None:
            from_connector_name = 'generic-jdbc-connector'
        if to_link_name is None:
            to_link_name = 'odps_data'
        if to_connector_name is None:
            to_connector_name = 'dli-connector'
        if to_db is None:
            to_db = 'yishou_data'
        if to_table is None:
            to_table = f'ods_{mysql_table}_temp'
        if shouldClearTable is None:
            shouldClearTable = 'true'
        if cdm_job_name is None:
            cdm_job_name = f'{to_table}_临时表_CDM任务'
        if insert_dt is None:
            insert_dt = 'eyLjbGVhclR5cGUiOiLLTlNFUlRfT1ZFUldSSVRFIn0='	# 表示无需指定插入分区

        # 默认的, 无需配置
        extendedconfigsFromconfigvalues = ExtendedConfigs(
            name="fromJobConfig.extendedFields",
            value="eyLCYXRjaEpvYiI6ImZhbHNlIn0="
        )

        # 输入源配置
        listInputsConfigs = [
            Input(
                name="fromJobConfig.useSql",
                value="false"
            ),
            Input(
                name="fromJobConfig.schemaName",
                value=f"{mysql_db}"
            ),
            Input(
                name="fromJobConfig.tableName",
                value=f"{mysql_table}"
            ),
            Input(
                name="fromJobConfig.incrMigration",
                value="false"
            ),
            Input(
                name="fromJobConfig.columnList",
                value=f"{columns}"
            ),
            Input(
                name="fromJobConfig.allowNullValueInPartitionColumn",
                value="false"
            ),
            Input(
                name="fromJobConfig.cdc",
                value="false"
            ),
            Input(
                name="fromJobConfig.createOutTable",
                value="false"
            )
        ]

        # 输入源配置汇总
        listConfigsFromconfigvalues = [
            Configs(
                inputs=listInputsConfigs,
                name="fromJobConfig"
            )
        ]
        fromconfigvaluesJobs = ConfigValues(
            configs=listConfigsFromconfigvalues,
            extended_configs=extendedconfigsFromconfigvalues
        )

        # 作业相关配置
        listInputsConfigs1 = [
            Input(
                name="groupJobConfig.groupId",
                value="1"
            ),
            Input(
                name="groupJobConfig.groupName",
                value="DEFAULT"
            )
        ]
        listInputsConfigs2 = [
            Input(
                name="retryJobConfig.retryJobType",
                value="NONE"
            )
        ]
        listInputsConfigs3 = [
            Input(
                name="smnConfig.isNeedNotification",
                value="false"
            )
        ]
        listInputsConfigs4 = [
            Input(
                name="schedulerConfig.isSchedulerJob",
                value="false"
            ),
            Input(
                name="schedulerConfig.disposableType",
                value="NONE"
            )
        ]
        listInputsConfigs5 = [
            Input(
                name="throttlingConfig.numExtractors",
                value="1"
            ),
            Input(
                name="throttlingConfig.submitToCluster",
                value="false"
            ),
            Input(
                name="throttlingConfig.numLoaders",
                value="1"
            ),
            Input(
                name="throttlingConfig.recordDirtyData",
                value="false"
            ),
            Input(
                name="throttlingConfig.writeToLink",
                value="OBS"
            ),
            Input(
                name="throttlingConfig.maxErrorRecords",
                value="10"
            )
        ]
        # 作业配置汇总
        listConfigsDriverconfigvalues = [
            Configs(
                inputs=listInputsConfigs1,
                name="groupJobConfig"
            ),
            Configs(
                inputs=listInputsConfigs2,
                name="retryJobConfig"
            ),
            Configs(
                inputs=listInputsConfigs3,
                name="smnConfig"
            ),
            Configs(
                inputs=listInputsConfigs4,
                name="schedulerConfig"
            ),
            Configs(
                inputs=listInputsConfigs5,
                name="throttlingConfig"
            )
        ]
        driverconfigvaluesJobs = ConfigValues(
            configs=listConfigsDriverconfigvalues
        )
        extendedconfigsToconfigvalues = ExtendedConfigs(
            name="toJobConfig.extendedFields",
            value=insert_dt
        )
        # 输出源配置
        listInputsConfigs6 = [
            Input(
                name="toJobConfig.queue",
                value=f"{queue}"
            ),
            Input(
                name="toJobConfig.database",
                value=f"{to_db}"
            ),
            Input(
                name="toJobConfig.table",
                value=f"{to_table}"
            ),
            Input(
                name="toJobConfig.columnList",
                value=f"{columns}"
            ),
            Input(
                name="toJobConfig.shouldClearTable",
                value=f"{shouldClearTable}"
            )
        ]
        listConfigsToconfigvalues = [
            Configs(
                inputs=listInputsConfigs6,
                name="toJobConfig"
            )
        ]
        toconfigvaluesJobs = ConfigValues(
            configs=listConfigsToconfigvalues,
            extended_configs=extendedconfigsToconfigvalues
        )
        listJobsbody = [
            Job(
                job_type="NORMAL_JOB",
                from_connector_name=f"{from_connector_name}",
                to_config_values=toconfigvaluesJobs,
                to_link_name=f"{to_link_name}",
                driver_config_values=driverconfigvaluesJobs,
                from_config_values=fromconfigvaluesJobs,
                to_connector_name=f"{to_connector_name}",
                name=f"{cdm_job_name}",
                from_link_name=f"{from_link_name}"
            )
        ]
        request.body = CdmCreateJobJsonReq(
            jobs=listJobsbody
        )
        response = self.client.create_job(request)
        print(response)
