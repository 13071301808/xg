# coding=utf-8
import string
from yssdk.dli.sql_client import YsDliSQLCLient


if __name__ == '__main__':
    # 执行单条SQL
    content = r'''
    select 1 as id,${bizdate} as rundate
    '''
    template_sql = string.Template(content)
    bizdate = '20220512'
    sql = template_sql.safe_substitute(bizdate=bizdate)
    client = YsDliSQLCLient(conf_mode='dev')
    client.exec_sql(sql)
    print(client.fetch_all())

    # 执行多条SQL
    content = r'''
    select 1 as id,${bizdate} as rundate;
    select 2 as id,${bizdate} as rundate
    '''
    template_sql = string.Template(content)
    bizdate = '20220512'
    sql = template_sql.safe_substitute(bizdate=bizdate)
    client = YsDliSQLCLient(conf_mode='dev')
    client.exec_multi_sql(sql)
    ## 返回最后一条查询语句的结果
    print(client.fetch_all())

    # 执行单条SQL,返回pandas dataframe
    content = r'''
    select 1 as id,${bizdate} as rundate
    '''
    template_sql = string.Template(content)
    bizdate = '20220512'
    sql = template_sql.safe_substitute(bizdate=bizdate)
    client = YsDliSQLCLient(conf_mode='dev')
    client.exec_sql(sql)
    print(client.fetch_all_dataframe())




