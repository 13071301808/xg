# coding=utf-8
import os
import configparser
from yssdk.constant.config import conf_dir

class Config:
    def __init__(self):
        pass

    @staticmethod
    def get_config(conf_mode = 'prod'):
        conf = configparser.RawConfigParser()
        if conf_mode == 'dev':
            conf.read(os.path.abspath(os.path.join(os.path.dirname("__file__"),os.path.pardir))+'/conf_dev.ini')
        elif conf_mode == 'prod':
            conf.read(conf_dir)
        return conf

