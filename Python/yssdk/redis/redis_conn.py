from yssdk.common.config import Config
import redis

class RedisConn:
    '''
    redis 连接
    '''
    def __init__(self, conf_mode='prod'):
        self.__conf = Config.get_config(conf_mode)
        self.__host = self.__conf['redis']['redis_host']
        self.__port = int(self.__conf['redis']['redis_port'])
        self.__password = self.__conf['redis']['redis_password']

    def get_redis_conn(self,redis_db):
        conn = redis.Redis(host=self.__host, port=self.__port,db=redis_db,password=self.__password)
        return conn