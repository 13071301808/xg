# 导入依赖包
import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


# 请求头
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
}
payload = ''
url = 'http://test3.yishou.com/xdebug/index/create_sql'


# 获取数据
def get_data():
    # 获取请求
    # response = requests.request("POST", url, headers=headers, data=payload)
    response = requests.get(url)
    # 设置字符编码格式
    response.encoding = 'utf-8'
    # 使用BeautifulSoup以lxml解析器的方式将数据进行解析
    soup = BeautifulSoup(response.text, 'lxml')
    # 提取网页中有class为pretty的div里面的内容
    result = soup.find_all('pre')
    ls = []
    for sql in result:
        createSQL = sql.text
        # 解析表名
        table_name_pattern = re.compile(r"CREATE\s+TABLE\s+`(\w+)`")
        table_name_match = table_name_pattern.search(createSQL)
        table_name = table_name_match.group(1)
        # 解析字段名和注释
        fields_pattern = re.compile(r"`(\w+)`\s+(\w+)\s+.*COMMENT\s+'(.*)'")
        fields_matches = fields_pattern.findall(createSQL)
        # 输出结果
        for field in fields_matches:
            # 数据库名称
            database_name = 'yishou_os'
            # 字段名
            field_name = field[0]
            # 类型
            type = field[1]
            # 备注
            description = field[2]
            # 是否分区键
            is_partition = 0
            # 更新时间
            update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # print(f"数据库名：{database_name},表名：{table_name},字段名：{field_name},类型：{type},注释：{description},是否分区：{is_partition},更新时间：{update_time}")
            ls.append([database_name, table_name, field_name, type, description, is_partition, update_time])
    # 以dataframe形式存储
    df = pd.DataFrame(ls, columns=['database_name', 'table_name', 'field_name', 'type', 'description', 'is_partition',
                                   'update_time'])
    return df
