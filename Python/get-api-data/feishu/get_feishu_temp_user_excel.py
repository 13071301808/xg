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
        # 获取现有的子表数量
        grid_properties_count = sum(1 for item in response_data['data']['sheets'] if 'grid_properties' in item)
        # print(grid_properties_count)
        # 定义多个表格id存放
        id_list = []
        # 通过循环遍历提取表格id(2个)
        for s_id in range(grid_properties_count):
            sheet_id = response_data['data']['sheets'][s_id]['sheet_id']
            sheet_name = response_data['data']['sheets'][s_id]['title']
            # 特殊抓取
            if 'sheet' in sheet_name.lower():
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
def get_sheet_data():
    sheet_ids = get_sheets_id()
    sheet_data = []
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
            for i in range(len(sheet_datas[1:])):
                i = i + 1
                if sheet_datas[i][0] is not None:
                    user_id = str(sheet_datas[i][0]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    user_id = None
                if sheet_datas[i][1] is not None:
                    remark = str(sheet_datas[i][1]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    remark = None
                if sheet_datas[i][2] is not None:
                    ex_1 = str(sheet_datas[i][2]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    ex_1 = None
                if sheet_datas[i][3] is not None:
                    ex_2 = str(sheet_datas[i][3]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    ex_2 = None
                if sheet_datas[i][4] is not None:
                    ex_3 = str(sheet_datas[i][4]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    ex_3 = None
                if sheet_datas[i][5] is not None:
                    ex_4 = str(sheet_datas[i][5]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    ex_4 = None
                if sheet_datas[i][6] is not None:
                    ex_5 = str(sheet_datas[i][6]).strip().replace('\n', ' ').replace(',', '.').replace('，', '.')
                else:
                    ex_5 = None

                # 提取表格字段数据
                sheet_info = {
                    'user_id': user_id,
                    'remark': remark,
                    'ex_1': ex_1,
                    'ex_2': ex_2,
                    'ex_3': ex_3,
                    'ex_4': ex_4,
                    'ex_5': ex_5,
                }
                sheet_data.append(sheet_info)

        # 以dataFrame形式存储
        dictionary_data = pd.DataFrame(sheet_data)
    except Exception as e:
        print(f"获取表格数据发生错误：{str(e)}")
    return dictionary_data


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


if __name__ == '__main__':
    table_id, db_name, table_name, is_dt, is_dli_dt, is_dli_create = get_args()
    # table_id = 'HfPWsfperhvzfGtUAk1co4sDnTb'
    # is_dli_create = 'false'
    dictionary_data = get_sheet_data()
    print(dictionary_data)
