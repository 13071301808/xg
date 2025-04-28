## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2024/11/07 20:30:49 GMT+08:00
## ******************************************************************** ##
"""
目的：为满足飞书在线文档同步到数仓，需要开发通用脚本将文档内容转存
使用前，请注意以下几点：
1、开放在线文档的链接分享权限，调为组织内获得链接的人可阅读
2、文档中的单元格格式不能为日期，统一为字符串或数值
3、支持自动建dli表，但注意表头不要太怪异，至少一个单元格内不要带有逗号

测试：
table_id = 'BmJdsO8qTh1bg7tvS65cCxgFnGb'
db_name = 'yishou_data'
table_name = 'dim_xiaohongshu_test'
"""

# 导入依赖包
import json
import requests
import pandas as pd
import argparse
import csv
from datetime import datetime, timedelta
from obs import ObsClient
from yssdk.dli.sql_client import YsDliSQLCLient

# 全局变量配置
pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', None)  # 设置列数为无限制
pd.set_option('display.width', 1000)  # 设置列宽
pd.set_option('display.colheader_justify', 'left')
ak = 'TWXI4V3FPVN8RGQY4BUO'
sk = 'hN2Fl8CLHknsi4zNhNkRosfIHx3O0cx3cNspIgJu'
my_server = 'https://obs.cn-south-1.myhuaweicloud.com'
obsClient = ObsClient(access_key_id=ak, secret_access_key=sk, server=my_server)


# 传入参数封装
def get_args():
    parser = argparse.ArgumentParser(description='将飞书在线文档同步至obs的相关参数')
    parser.add_argument('-table_id', type=str, required=True, help='表格唯一id（必填）')
    parser.add_argument('-db_name', type=str, required=True, help='目标端数据库名（必填）')
    parser.add_argument('-table_name', type=str, required=True, help='目标端数据表名（必填）')
    parser.add_argument('--is_dt', type=str, default=0, help='是否为分区表，默认为全量表')
    parser.add_argument('--is_dli_dt', type=str, default=0, help='是否建dli分区表，默认为不建')
    parser.add_argument('--is_dli_create', type=str, default='false', help='是否建dli表，默认为不建')
    args = parser.parse_args()
    return args.table_id, args.db_name, args.table_name, args.is_dt, args.is_dli_dt, args.is_dli_create


# 自动获取最新的token
def get_authorization():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": "cli_a46f1952bb79d00c",
        "app_secret": "s9xm9RJ7KAdxlKMIzBZ0ofWYpsdG3Wao"
    })
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    Authorization = response_data['tenant_access_token']
    return Authorization


# 请求头
headers = {
    'Authorization': 'Bearer ' + get_authorization(),
    'Content-Type': 'application/json'
}


# 获取表格id
def get_sheets_id():
    try:
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{table_id}/sheets/query"
        payload = ''
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        # 定义多个表格id存放
        id_list = []
        # 通过循环遍历提取表格id(2个)
        for s_id in range(1):
            sheet_id = response_data['data']['sheets'][s_id]['sheet_id']
            # 存放表格id与名字
            sheet_id_info = {
                'sheet_id': sheet_id
            }
            id_list.append(sheet_id_info)
        id_list_pd = pd.DataFrame(id_list)
        print(id_list_pd)
    except Exception as e:
        print(f"获取表格id发生错误：{str(e)}")
    return id_list_pd


# 查找单元格信息
def get_sheet_data(is_dli_create):
    sheet_ids = get_sheets_id()
    dictionary_data = pd.DataFrame()
    try:
        # 将获取的id提取相应表格的数据
        for s_id in range(len(sheet_ids)):
            url = (
                f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{table_id}/values/{sheet_ids.iloc[s_id, 0]}'
                f'?valueRenderOption=ToString&valueRenderOption=FormattedValue'
            )
            payload = ''
            response1 = requests.request("GET", url, headers=headers, data=payload)
            response_data1 = json.loads(response1.text)
            # 提取表格数据
            sheet_datas = response_data1['data']['valueRange']['values']
            # 处理每个单元格的异常值
            for row in sheet_datas:
                for i in range(len(row)):
                    # 转换为字符串并去除前后空格
                    cell_value = row[i]
                    if cell_value is None:
                        cell_value = 'null'
                    else:
                        # 替换换行符和回车符为空格
                        cell_value = str(cell_value).replace('\n', ' ').replace('\r', ' ')
                    row[i] = cell_value
    except Exception as e:
        print(f"获取表格数据发生错误：{str(e)}")
    # 按是否建表区分情况
    if is_dli_create == 'false':
        dictionary_data = (pd.DataFrame(sheet_datas)).drop(0)
    elif is_dli_create == 'true':
        dictionary_data = (pd.DataFrame(sheet_datas, columns=sheet_datas[0])).drop(0)
    return dictionary_data


# 判断是否要建dli表
def is_dli_create_table(dictionary_data, db_name, table_name, is_dli_create):
    if is_dli_create == 'false':
        print('不用建表，只放到obs')
    elif is_dli_create == 'true':
        columns_list = list(dictionary_data.columns)
        # 动态生成字段定义，每个字段类型为string
        columns_definition = ',\n    '.join([f'`{col}` string' for col in columns_list])
        # 构造建表SQL
        location_path = f'obs://yishou-bigdata/{db_name}.db/{table_name}'  # 动态LOCATION路径
        create_sql = f'''
            CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{table_name} (
                {columns_definition}
            )
            COMMENT '飞书同步表格数仓表'
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
                'separatorChar' = ',',
                'quoteChar' = '"',
                'escapeChar' = '\\\\'
            )
            STORED AS TEXTFILE
            LOCATION '{location_path}'
            ;
        '''
        print('建表语句:\n', create_sql)
        # 执行SQL创建表
        dli_client = YsDliSQLCLient(queue='develop')
        dli_client.exec_sql(create_sql)


def insert_data(dictionary_data, is_dt):
    # 堡垒机的路径
    file_path = fr"/apps/data/project/tmp/{table_name}.csv"
    # 保存到堡垒机
    dictionary_data.to_csv(file_path, index=False, header=False, encoding='utf8', na_rep='null')
    obs_dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    # 判断是否分区
    if is_dt == 0:
        # 对象名，即上传后的文件名
        objectKey = f"{db_name}.db/{table_name}/" + f"{table_name}.csv"
        # obsClient.putFile是追加上传而不是覆盖
        up_resp = obsClient.putFile("yishou-bigdata", objectKey, file_path)
        print('检测为非分区表')
    elif is_dt == 1:
        obs_dt = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        # 对象名，即上传后的文件名(日期目录+当日文件名)
        objectKey = f"{db_name}.db/{table_name}/dt=%s/" % obs_dt + obs_dt + ".csv"
        up_resp = obsClient.putFile("yishou-bigdata", objectKey, file_path)
        print('检测为分区表')
    # 检查写入华为obs是否已成功
    if up_resp.status < 300:
        print('obs没问题')
    else:
        print('寄啦,没写到obs')


# 判断是否需要建dli分区表的文件夹
def is_dli_table(db_name, table_name, is_dli_dt):
    if is_dli_dt == 0:
        print('不创建dli表，无需执行mark')
    elif is_dli_dt == 1:
        dli_client = YsDliSQLCLient(queue='develop')
        dli_client.exec_sql(f"MSCK REPAIR TABLE {db_name}.{table_name}")


if __name__ == '__main__':
    table_id, db_name, table_name, is_dt, is_dli_dt, is_dli_create = get_args()
    dictionary_data = get_sheet_data(is_dli_create)
    is_dli_create_table(dictionary_data, db_name, table_name, is_dli_create)
    insert_data(dictionary_data, is_dt)
    is_dli_table(db_name, table_name, is_dli_dt)
