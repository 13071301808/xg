# coding=utf-8
import os

from obs import ObsClient
import datetime,time
from yssdk.common.printter import Printter
from yssdk.common.config import Config
import traceback


class YsObsClient:
    """
    Yishou OBS 客户端
    """

    def __init__(self, conf_mode='prod'):
        conf = Config.get_config(conf_mode)
        self.obs_client = ObsClient(
            access_key_id=conf['dli_conf']['ys_ak'],
            secret_access_key=conf['dli_conf']['ys_sk'],
            server=conf['obs_conf']['ys_obs_server']
        )

    def upload_to_obs(self, obs_bucket, obs_target_path, local_path):
        """
        把本地文件上传到OBS
        :param obs_bucket: OBS bucket
        :param obs_target_path: OBS路径, 如 yishou_data.db/all_test_table
        :param local_path: 本地路径
        :return:
        """
        try:
            Printter.info("Upload Local file %s to OBS %s" % (local_path, obs_target_path))
            resp = self.obs_client.putFile(obs_bucket, obs_target_path, local_path)
            Printter.info("Response status: %s" % resp.status)

            if resp.status > 300:
                Printter.error('errorCode: %s' % (resp.errorCode))
                Printter.error('errorMessage: %s' % (resp.errorMessage))
            else:
                Printter.info('Uploaded successfully')
        except:
            Printter.error(traceback.format_exc())

    def list_obs_dir(self, obs_bucket, obs_dir):
        """
        列出OBS目录下所有对象
        :param obs_bucket: OBS bucket
        :param obs_dir: OBS 目录， 如 yishou_data.db/all_test_table
        :return: 列表
        """
        dir_list = []
        try:
            resp = self.obs_client.listObjects(obs_bucket, prefix=obs_dir, max_keys=100)
            if resp.status < 300:

                for content in resp.body.contents:
                    dir_list.append(content.key)
            else:
                Printter.error('errorCode:', resp.errorCode)
                Printter.error('errorMessage:', resp.errorMessage)
        except:
            Printter.error(traceback.format_exc())

        distinct_dir_list = sorted(set(dir_list))
        distinct_dir_list.reverse()
        return distinct_dir_list

    def delete_obs_object(self, obs_bucket, obs_object):
        """
        删除OBS对象
        :param obs_bucket: OBS bucket
        :param obs_object: OBS 对象
        :return: None
        """
        try:
            Printter.info('Delete OBS file: %s' % dir)
            resp = self.obs_client.deleteObject(obs_bucket, obs_object)

            if resp.status < 300:
                Printter.info('OBS Object %s has been deleted' % obs_object)
            else:
                Printter.error('Failed to delete OBS Object %s' % obs_object)
                Printter.error('errorCode: %s' % resp.errorCode)
                Printter.error('errorMessage: %s' % resp.errorMessage)
        except:
            Printter.error(traceback.format_exc())

    def clean_date_partition_table_obs_data(self, obs_bucket, obs_dir, retention_days):
        """
        清理OBS数据，适用于按日期分区的表，如dt=20230101
        :param obs_bucket:OBS bucket
        :param obs_dir:OBS目录，如 yishou_data.db/all_test_table
        :param retention_days: 数据保留天数
        :return: None
        """
        Printter.info('Clean OBS data for directory: %s' % obs_dir)
        dir_set = self.list_obs_dir(obs_bucket, obs_dir)
        #yesterday = int((datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
        yesterday = (datetime.date.today() + datetime.timedelta(days=-1))
        try:
            for dir in dir_set:
                if os.path.dirname(dir) == obs_dir:
                    continue
                pos = dir.find('dt=')
                #exec_date = int(os.path.dirname(dir).split('=')[1])
                exec_date_str = dir[pos:-1].split('/')[0].split('=')[1]
                exec_date = datetime.datetime.strptime(exec_date_str, '%Y%m%d').date()
                delta = (yesterday - exec_date).days
                if delta >= retention_days:
                    self.clean_obs_data_file(obs_bucket,dir)
                    time.sleep(1)
                    self.delete_obs_object(obs_bucket, dir)
        except Exception as e:
            import traceback
            Printter.error(traceback.format_exc())


    def clean_obs_data_file(self, obs_bucket, obs_dir):
        """
        删除给定OBS目录下的所有文件
        :param obs_bucket:OBS bucket
        :param obs_dir:OBS目录，如 yishou_data.db/all_test_table
        :return: None
        """
        Printter.info('Clean OBS data for directory: %s' % obs_dir)
        dir_set = self.list_obs_dir(obs_bucket, obs_dir)
        count = 0
        for dir in dir_set:
            count += 1
            if count >= 20:
                time.sleep(1)
                count = 0
            self.delete_obs_object(obs_bucket, dir)

    def create_obs_dir(self, obs_bucket, obs_dir):
        """
        创建OBS目录
        :param obs_bucket: OBS bucket
        :param obs_dir:OBS目录
        :return: None
        """
        resp = self.obs_client.putContent(obs_bucket, obs_dir, content=None)
        if resp.status < 300:
            Printter.info('requestId:', resp.requestId)
            Printter.info('OBS dir created successfully')
        else:
            Printter.error('OBS dir created with failure')
            Printter.error('errorCode:', resp.errorCode)
            Printter.error('errorMessage:', resp.errorMessage)

    def get_obs_dir_size(self, obs_bucket, obs_dir):
        """
        获取指定OBS目录的大小
        :param obs_bucket: OBS bucket
        :param obs_dir: OBS目录
        :return:
        """
        dir_set = self.list_obs_dir(obs_bucket, obs_dir)
        length = 0
        for dir in dir_set:
            if os.path.dirname(dir) == obs_dir:
                continue
            resp = self.obs_client.getObjectMetadata(obs_bucket, dir)
            length += int(resp['body']['contentLength'])
        Printter.info('size of %s is '%obs_dir, str(length),'byte')
        return length

    def check_if_obs_file_object_exists(self, obs_bucket, obs_object):
        """
        查看OBS对象是否存在
        :param obs_bucket: OBS bucket
        :param obs_object: OBS 对象
        :return:bool True|False
        """

        try:
            resp = self.obs_client.getObjectMetadata(obs_bucket, obs_object)
            if resp.status < 300:
                Printter.info("OBS Object %s is exists"%obs_object)
                return True
            else:
                Printter.warn("OBS Object %s is not exists"%obs_object)
                return False
        except:
            import traceback
            print(traceback.format_exc())


    def get_obs_client(self):
        """
        获取华为云obs client,
        :return: 华为云ObsClient对象
        """
        return self.obs_client

if __name__ == '__main__':
    obs_client = YsObsClient(conf_mode='dev')

    obs_client.get_obs_dir_size('yishou-bigdata','yishou_data.db/all_fmys_order/')
    #obs_client.get_metadata('yishou-bigdata','yishou_data.db/all_fmys_order/part-00044-aa26d1ea-05f9-4e14-b225-b542d7ee79ed-c000')
    pass