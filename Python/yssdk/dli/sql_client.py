# coding=utf-8
from pyDLI import dli
from yssdk.common.printter import Printter
from yssdk.common.config import Config
import datetime
import pandas as pd
import traceback
import sys



class YsDliSQLCLient:
    def __init__(self, conf_mode='prod', queue='develop', database='default', options=None):
        conf = Config.get_config(conf_mode)
        endpoint = conf['dli_conf']['ys_endpoint']
        project_id = conf['dli_conf']['ys_project_id']
        ak = conf['dli_conf']['ys_ak']
        sk = conf['dli_conf']['ys_sk']
        auth_mode = conf['dli_conf']['ys_auth_mode']
        queue = queue
        database = database

        default_options = {'dli.sql.dynamicPartitionOverwrite.enabled':'true'}
        self.sql_options = default_options.copy()
        if options is not None:
            self.sql_options.update(options)

        Printter.info("Init DLI Connection...")
        self.conn = dli.Connection(
            host="dli://%s/%s?queuename=%s&database=%s" % (endpoint, project_id, queue, database), ak=ak, sk=sk,
            auth=auth_mode)
        self.cursor = self.conn.cursor()

    def get_connection(self):
        """
        获取DLI connetion对象
        :return: DLI connetion对象
        """
        Printter.info("Get DLI Connection...")
        return self.conn

    def print_result(self):
        """
        打印查询返回的记录
        :return: None
        """
        Printter.info("schema info:")
        for col_info in self.cursor.description:
            Printter.info("\t%s" % (col_info,))
        Printter.info("job result:")
        result_list = self.cursor.fetchall()
        for row_data in result_list:
            Printter.info("\t%s" % row_data)
        Printter.info(self.cursor.rownumber)

    def exec_sql(self, sql, parameters=None, options=None, output=True):
        """
        执行单条 DLI SQL
        :param sql: 单条SQL语句
        :param parameters: sequence or mapping parameters: Input parameters which will be bound to variables in the operation
        :param options: dict options: User-defined conf that applies to the sql job
        :param output: boolean: Used to define whether to output exe_sql
        :return: None
        """
        sql_options = self.sql_options.copy()
        if options is not None:
            sql_options.update(options)
        if output:
            Printter.info("Execute DLI SQL:" + " ".join(sql.split()))
        try:
            self.cursor.execute(sql, parameters, sql_options)
        except Exception as e:
            sys.exit(traceback.format_exc())

    def fetch_all(self):
        """
        拉取resultset所有数据
        :return: list类型
        """
        return self.cursor.fetchall()

    def fetch_one(self):
        """
        拉取resultset一条数据
        :return: list类型
        """
        return self.cursor.fetchone()

    def exec_multi_sql(self, sqls, parameters=None, options=None):
        """
        执行 DLI SQL
        :param sqls: 支持单挑或者多条 DLI SQL语句, 多条SQL时用分号;分隔
        :param parameters: sequence or mapping parameters: Input parameters which will be bound to variables in the operation
        :param options: dict options: User-defined conf that applies to the sql job
        :return: None
        """
        #try:
        #    sql_list = sqls.strip().split(';')
        #    for sql in sql_list:
        #        sql = sql.strip()
        #        ## 过滤空行以及注释
        #        if len(sql) > 0 and sql[0:2] != '--':
        #            self.exec_sql(sql)
        #except Exception as e:
        #    sys.exit(traceback.format_exc())
        try:
            lines = sqls.split('\n')
            multi_sql = ''
            for line in lines:
                # 过滤空行以及注释
                if len(line) > 0 and line[0:2] == '--':
                    continue
                multi_sql += line + '\n'

            # 用分号分割sql
            sql_list = multi_sql.split(';')
            for sql in sql_list:
                if len(sql.strip()) > 0:
                    self.exec_sql(sql)
        except Exception as e:
            sys.exit(traceback.format_exc())


    def fetch_all_dataframe(self):
        """
         返回resultset的所有数据，并转化为pandas dataframe对象
        :return: dataframe
        """
        try:
            column_list = [column.name for column in self.cursor._sql_job.get_schema()]
            data = list(self.cursor.fetchall())
            return pd.DataFrame(data, columns=column_list)
        except Exception as e:
            sys.exit(traceback.format_exc())

    def get_partitions(self, database, table):
        """
        获取表的分区
        :param database: DLI的库名
        :param table: DLI的表名
        :return: 分区, list类型
        """
        Printter.info("Get table %s.%s partition"%(database, table))
        try:
            sql = "show partitions %s.%s" % (database, table)
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            sys.exit(traceback.format_exc())

    def clean_partitions(self, database, table, retention_days):
        """
        清理分区
        :param database: DLI的库名
        :param table: DLI的表名
        :param retention_days: 保留天数
        :return: None
        """

        try:
            partition_list = self.get_partitions(database, table)
            #yesterday = int((datetime.date.today() + datetime.timedelta(days=-1)).strftime('%Y%m%d'))
            yesterday = (datetime.date.today() + datetime.timedelta(days=-1))
            deleted_partition = []
            for partition_str in partition_list:
                sql = "alter table %s.%s drop partition(" % (database, table)
                partition = partition_str[0].split(':')[0].strip()
                #exec_date = int(partition.split('=')[1])
                #exec_date = int(partition.split('/')[0].split('=')[1])
                exec_date_str = partition.split('/')[0].split('=')[1]
                exec_date = datetime.datetime.strptime(exec_date_str, '%Y%m%d').date()
                delta = (yesterday - exec_date).days
                date_partition = partition.split('/')[0]
                if delta >= retention_days and date_partition not in deleted_partition:
                    sql += date_partition + ')'
                    Printter.info('Operation: %s' % sql)
                    self.cursor.execute(sql)
                    deleted_partition.append(date_partition)
        except Exception as e:
            sys.exit(traceback.format_exc())

    def msck_repair_table(self, database, table):
        """
        msck更新表的元数据
        :param database: DLI的库名
        :param table: DLI的表名
        :return: None
        """

        sql = 'msck repair table %s.%s' % (database, table)
        Printter.info('Operation: %s' % sql)
        try:
            self.cursor.execute(sql)
        except Exception as e:
            sys.exit(traceback.format_exc())

    def get_table_obs_location(self, database, table):
        """
        获取DLI表的OBS存储路径
        :return:
        """
        sql = 'show create table %s.%s'%(database, table)

        Printter.info('Operation: %s'%(sql))
        try:
            self.cursor.execute(sql)
            ddl = self.cursor.fetchall()[0][0]
            items = ddl.split('\n')
            for item in items:
                if item.find('LOCATION') > -1:
                    obs_location = item.split(' ')[1]
                    return obs_location
        except Exception as e:
            sys.exit(traceback.format_exc())

    def get_dli_table_partition_columns(self, database, table):
        """
        获取DLI表的分区字段信息，包括分区字段名，字段类型，字段注释
        :param database:
        :param table:
        :return:
        """
        sql = 'show create table %s.%s'%(database, table)
        Printter.info('Operation: %s'%(sql))
        partition_columns = []
        try:
            self.cursor.execute(sql)
            ddl = self.cursor.fetchall()[0][0]
            items = ddl.split('\n')
            for item in items:
                if item.find('PARTITIONED') > -1:
                    partition_columns_list = item.replace('PARTITIONED BY','').replace('(','').replace(')','').replace('`','').split(',')
                    for partition in partition_columns_list:
                        partition_column = {}
                        infos = partition.split(' ')
                        field_name = infos[1].lower()
                        field_type = infos[2].lower()
                        #field_comment = infos[4]
                        partition_column['field_name'] = field_name
                        partition_column['field_type'] = field_type
                        #partition_column['field_comment'] = field_comment
                        partition_columns.append(partition_column)
        except Exception as e:
            sys.exit(traceback.format_exc())

        return(partition_columns)

    def get_dli_table_columns(self, database, table):
        """
        获取DLI表的字段信息，包括字段名，字段类型，字段注释
        :param database:
        :param table:
        :return:
        """
        sql = 'desc %s.%s'%(database, table)
        Printter.info('Operation: %s'%(sql))
        columns = []
        partition_column_list = []
        for partition_column_info in self.get_dli_table_partition_columns(database, table):
            partition_column_list.append(partition_column_info['field_name'])

        try:

            self.cursor.execute(sql)
            columns_info = self.cursor.fetchall()

            for column_info in columns_info:
                column = {}
                if column_info[0].find('Partition') > 0:
                    break
                #if partition_column_list.count(column_info[0]) > 0:
                #    continue
                column['field_name'] = column_info[0].lower()
                column['field_type'] = column_info[1].lower()
                column['field_comment'] = column_info[2]
                columns.append(column)
        except Exception as e:
            sys.exit(traceback.format_exc())

        return(columns)

    def generate_gaussdb_dws_ddl(self, database, table, gaussdb_db_name, gassdb_table_name):
        """
        根据DLI的表生成GaussDB DWS的建表语句
        :param database:
        :param table:
        :param gaussdb_db_name:
        :param gassdb_table_name:
        :return:
        """
        dli_gaussdb_data_type_mappping = {
            'bigint':'bigint',
            'int':'int',
            'string':'character varying',
            'double':'double precision'
        }
        ddl_sql = 'create foreign table if not exists %s.%s\n'%(gaussdb_db_name, gassdb_table_name)
        ddl_sql += '(\n'
        columns = self.get_dli_table_columns(database, table)
        columns_num = len(columns)
        obs_location = (self.get_table_obs_location(database,table).replace('obs:/','') + '//').replace("'//","/'")
        partition_column_list = []
        for partition_column_info in self.get_dli_table_partition_columns(database, table):
            partition_column_list.append(partition_column_info['field_name'])

        for column in columns:
            ddl_sql += '%s %s'%(column['field_name'], dli_gaussdb_data_type_mappping[column['field_type']])
            columns_num -= 1
            if columns_num > 0:
                ddl_sql += ','
            ddl_sql += '\n'
        ddl_sql += ')\n'
        ddl_sql += 'SERVER obs_server\n'
        ddl_sql += "OPTIONS (encoding 'utf8', foldername %s, format 'orc', totalrows '5000')\n"%(obs_location)
        ddl_sql += 'distribute by ROUNDROBIN\n'
        if len(partition_column_list) > 0:
            ddl_sql += 'PARTITION BY (%s)\n'%(','.join(partition_column_list))
        ddl_sql += ';\n'

        ## 生成注释
        for column in columns:
            if column['field_comment'] is None:
                continue
            comment = "COMMENT ON COLUMN %s.%s.%s IS '%s';\n"%(gaussdb_db_name, gassdb_table_name, column['field_name'], column['field_comment'])
            ddl_sql += comment

        return ddl_sql

if __name__ == '__main__':
    pass


