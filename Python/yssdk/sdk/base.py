# -*-coding: Utf-8 -*-
# @File : base .py
# author: ShanFeng
# Time：2024/3/5/0005
from huaweicloudsdkcore.auth.credentials import BasicCredentials
from yssdk.common.config import Config
from yssdk.common.printter import Printter


class SDK_Client(object):
    """
    作为各sdk工具的父类, 初始化获取数据
    """
    def __init__(self):
        self.__conf = Config.get_config()
        ak = self.__conf['dli_conf']['ys_ak']
        sk = self.__conf['dli_conf']['ys_sk']
        self.__region = self.__conf['dli_conf']['ys_region']
        self.__credentials = BasicCredentials(ak, sk)
        self.printter = Printter()

    def get_credentials(self):
        return self.__credentials

    def get_region(self):
        return self.__region

    def get_cluster_id(self, cluster_name):
        self.__cluster_id = self.__conf['cdm_conf'][cluster_name]
        return self.__cluster_id
