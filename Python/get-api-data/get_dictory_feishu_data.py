# 导入依赖包
import json
import requests
import pandas as pd
from datetime import datetime, timedelta

from yssdk.mysql.client import YsMysqlClient

pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', None)  # 设置列数为无限制
pd.set_option('display.width', 1000)  # 设置列宽
pd.set_option('display.colheader_justify', 'left')

one_day_ago = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")


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
        url = "https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/shtcn2R1LgjPffruJR8fwUBWqGf/sheets/query"
        payload = ''
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        # 定义多个表格id存放
        id_list = []
        # 通过循环遍历提取表格id(9个)
        for s_id in range(9):
            sheet_id = response_data['data']['sheets'][s_id + 2]['sheet_id']
            sheet_name = response_data['data']['sheets'][s_id + 2]['title']
            # 存放表格id与名字
            sheet_id_info = {
                'sheet_id': sheet_id,
                'sheet_name': sheet_name,
            }
            id_list.append(sheet_id_info)
        id_list_pd = pd.DataFrame(id_list)
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
                f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/shtcn2R1LgjPffruJR8fwUBWqGf/values/{sheet_ids.iloc[s_id, 0]}'
                f'?valueRenderOption=ToString&valueRenderOption=FormattedValue'
            )
            payload = ''
            response1 = requests.request("GET", url, headers=headers, data=payload)
            response_data1 = json.loads(response1.text)

            # 提取表格数据
            sheet_datas = response_data1['data']['valueRange']['values']

            for i in range(len(sheet_datas[1:])):
                i = i + 1
                # 单独对business_caliber进行文字数据处理
                business_caliber = sheet_datas[i][2]
                # 单独对technical_caliber进行文字数据处理
                technical_caliber = sheet_datas[i][3]
                # 单独对comment进行文字数据处理
                comment = sheet_datas[i][4]
                # 更新时间
                update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # 提取表格字段数据
                sheet_info = {
                    'sheet_name': sheet_ids.iloc[s_id, 1],
                    'target_type': sheet_datas[i][0],
                    'target_name': sheet_datas[i][1],
                    'business_caliber': business_caliber,
                    'technical_caliber': technical_caliber,
                    'comment': comment,
                    'update_time': update_time
                }
                sheet_data.append(sheet_info)
        # 以dataFrame形式存储
        dictionary_data = pd.DataFrame(sheet_data)
        # 重命名列名
        dictionary_data = dictionary_data.rename(
            columns={
                0: 'sheet_name', 1: 'target_type', 2: 'target_name', 3: 'business_caliber',
                4: 'technical_caliber', 5: 'comment', 6: 'update_time'
            }
        )
        # print(dictionary_data)
    except Exception as e:
        print(f"获取表格数据发生错误：{str(e)}")
    return dictionary_data


# 清空表数据
def truncate_data():
    mysql_client = YsMysqlClient()
    mysql_client.get_obconnect()
    sql1 = f'alter table dw_yishou_data.target_dictionary truncate partition p{one_day_ago}'
    mysql_client.execute_sql(sql1, False)
    mysql_client.commit()


# 添加数据入库
def insert_table():
    dictionary_data = get_sheet_data()
    mysql_client = YsMysqlClient()
    mysql_client.get_obconnect()

    for i in range(len(dictionary_data)):
        sheet_name = str(dictionary_data.iloc[i, 0]).replace('数据字典-', '')
        target_type = str(dictionary_data.iloc[i, 1])
        # 替换无用数据
        target_type = target_type.replace("'", '"').replace('None', '')
        target_name = str(dictionary_data.iloc[i, 2]).replace("'", '"')
        business_caliber = str(dictionary_data.iloc[i, 3])
        # 替换无用数据
        business_caliber = business_caliber.replace('（', '_').replace('）', '_') \
            .replace('。', '').replace("'", '"').replace('None', '')
        technical_caliber = str(dictionary_data.iloc[i, 4])
        # 替换无用数据
        technical_caliber = technical_caliber.replace("'", '"').replace('（', '_') \
            .replace('）', '_').replace('。', '').replace('None', '')
        comment = str(dictionary_data.iloc[i, 5])
        # 替换无用数据
        comment = comment.replace('（', '_').replace('）', '_') \
            .replace('None', '').replace("'", '"')
        update_time = str(dictionary_data.iloc[i, 6])
        # 定义sql语句
        sql = (
            "insert INTO dw_yishou_data.target_dictionary(sheet_name,target_type,target_name,business_caliber,"
            "technical_caliber,comment,update_time,dt) VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')".format(
                sheet_name, target_type, target_name, business_caliber, technical_caliber, comment, update_time,one_day_ago
            )
        )
        # 对入库添加try容错
        try:
            result = mysql_client.execute_sql(sql, False)
            mysql_client.commit()
        except Exception as e:
            print(f"入库发生错误：{str(e)}")


if __name__ == '__main__':
    truncate_data()
    insert_table()
