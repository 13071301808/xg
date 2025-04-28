# 导入依赖包
import json
import requests
import pandas as pd
import pymysql
from datetime import datetime


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


def get_dep_ids(user_id):
    url = 'https://open.feishu.cn/open-apis/contact/v3/users/' + user_id + '?department_id_type=open_department_id&user_id_type=open_id'
    payload = ''
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    return response_data['data']['user']['department_ids']


# 通过获取部门api获得部门与父部门信息
def get_dep(dep_id):
    # 定义部门字段
    f_dep_name = ''
    dep_name = ''
    f_dep_id = 0
    try:
        url = 'https://open.feishu.cn/open-apis/contact/v3/departments/' + dep_id + '?department_id_type=open_department_id&user_id_type=open_id'
        payload = ''
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        # 部门
        dep_list = response_data['data']['department']
        if 'name' in dep_list:
            dep_name = dep_list['name']
        # 父部门
        if 'parent_department_id' in dep_list:
            f_dep_id = dep_list['parent_department_id']
            url = 'https://open.feishu.cn/open-apis/contact/v3/departments/' + f_dep_id + '?department_id_type=open_department_id&user_id_type=open_id'
            payload = ''
            response = requests.request("GET", url, headers=headers, data=payload)
            response_data = json.loads(response.text)
            f_dep_name = response_data['data']['department']['name']
    except Exception as e:
        print(f"获取部门信息发生错误：{str(e)}")
    return [dep_name, f_dep_name, dep_id, f_dep_id]


# 通过获取用户api获得用户基本信息
def get_user(user_id, dep_id):
    url = 'https://open.feishu.cn/open-apis/contact/v3/users/' + user_id + '?department_id_type=open_department_id&user_id_type=open_id'
    payload = ''
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    user_list = response_data['data']['user']
    # 判断是否无花名，无花名的话花名就是真名，有花名就花名
    if 'custom_attrs' in user_list:
        real_name = user_list['custom_attrs'][0]['value']['text']
        # 判断custom_attrs长度，判断是否含有职位等级
        if len(user_list['custom_attrs']) >= 2:
            job_level = user_list['custom_attrs'][1]['value']['text']
        else:
            job_level = ''
    else:
        real_name = user_list['name']
        job_level = ''
    # 花名
    if 'name' in user_list:
        nick_name = user_list['name']
    else:
        nick_name = ''
    dep_id = dep_id
    # 部门
    dep_name = get_dep(dep_id)[0]
    # 父部门
    f_dep_name = get_dep(dep_id)[1]
    # 部门id
    dep_id = get_dep(dep_id)[2]
    # 父部门id
    f_dep_id = get_dep(dep_id)[3]
    # 职位
    if 'job_title' in user_list:
        job_name = user_list['job_title']
    else:
        job_name = ''
    # 手机号
    if 'mobile' in user_list:
        mobile = user_list['mobile']
    else:
        mobile = ''
    # 入职时间
    if 'join_time' in user_list:
        join_time_timestamp = user_list['join_time']
        # 将时间戳转换成日期数据
        join_time = datetime.fromtimestamp(join_time_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    else:
        join_time = ''
    # 获取当前时间
    update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return [user_id, real_name, nick_name, f_dep_id, dep_id, f_dep_name, dep_name, job_name, job_level, mobile,
            join_time, update_time]


class MysqlClient:
    def __init__(self, db, host, user, password):
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db, charset="utf8mb4")
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.close()

    def select(self, sql, args):
        self.cursor.execute(query=sql, args=args)
        return self.cursor.fetchall()

    # 删除、更新、插入
    def update(self, sql):
        temp = self.cursor.execute(sql)
        self.conn.commit()
        return temp

    def close(self):
        self.cursor.close()
        self.conn.close()


# 生成人事数据汇总
def shengcheng_data():
    n = 0
    # 每页显示的数量
    page_size = 10
    # 请求url
    url = f"https://open.feishu.cn/open-apis/ehr/v1/employees?page_size={page_size}&page_token={n * page_size}&view=full&status=2&status=4"
    payload = ''
    # 请求体
    response = requests.request("GET", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    # 存放用户所有信息的容器
    user_info_list = []
    # 判断遍历后后面是否存在数据
    while response_data['data']['has_more'] == True:
        for user in response_data['data']['items']:
            user_id = user['user_id']
            dep_ids = get_dep_ids(user_id)
            for dep_id in dep_ids:
                user_data = get_user(user_id, dep_id)
                user_info = {
                    '用户ID': user_data[0],
                    '真名': user_data[1],
                    '花名': user_data[2],
                    '父部门id': user_data[3],
                    '部门id': user_data[4],
                    '父部门': user_data[5],
                    '部门': user_data[6],
                    '职位': user_data[7],
                    '职位等级': user_data[8],
                    '手机号': user_data[9],
                    '入职时间': user_data[10],
                    '更新時間': user_data[11]
                }
                user_info_list.append(user_info)
        n = n + 1
        # 请求url
        url = f"https://open.feishu.cn/open-apis/ehr/v1/employees?page_size={page_size}&page_token={n * page_size}&view=full&status=2&status=4"
        payload = ''
        # 请求体
        response = requests.request("GET", url, headers=headers, data=payload)
        response_data = json.loads(response.text)
        # 转换成dataFrame形式
        huamingce = pd.DataFrame(user_info_list)
    return huamingce


# 查询数据测试
def select_data():
    pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq")
    sql2 = "select user_id,realname,nickname,f_dep_name,dep_name,job_name,join_time,update_time from ys_dgc.dgc_feishu_user"
    result2 = pymysql.select(sql2, [])
    result_pd2 = pd.DataFrame(result2)
    return result_pd2


# 清空表数据
def truncate_data():
    pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq")
    sql1 = "truncate table ys_dgc.dgc_feishu_user"
    result1 = pymysql.update(sql1)


# 插入表数据
def insert_data():
    huamingce = shengcheng_data()
    pymysql = MysqlClient("fmdes", "192.168.10.170", "fmdes_user", "6IveUZJyx1ifeWGq")
    for i in range(len(huamingce)):
        # 编写插入语句
        user_id = str(huamingce.iloc[i, 0])
        realname = str(huamingce.iloc[i, 1])
        nickname = str(huamingce.iloc[i, 2])
        f_dep_id = str(huamingce.iloc[i, 3])
        dep_id = str(huamingce.iloc[i, 4])
        f_dep_name = str(huamingce.iloc[i, 5])
        dep_name = str(huamingce.iloc[i, 6])
        job_name = str(huamingce.iloc[i, 7])
        job_level = str(huamingce.iloc[i, 8])
        mobile = str(huamingce.iloc[i, 9])[3:]
        join_time = str(huamingce.iloc[i, 10])
        update_time = str(huamingce.iloc[i, 11])
        sql = "replace INTO ys_dgc.dgc_feishu_user (user_id,realname,nickname,f_dep_id,dep_id,f_dep_name,dep_name,job_name,job_level,mobile,join_time,update_time) VALUES (\"" + user_id + "\" , \"" + realname + "\" , \"" + nickname + "\" , \"" + f_dep_id + "\",\"" + dep_id + "\",\"" + f_dep_name + "\" , \"" + dep_name + "\" , \"" + job_name + "\" ,\"" + job_level + "\",\"" + mobile + "\",\"" + join_time + "\" , \"" + update_time + "\")"
        # 对入库添加try容错
        try:
            result = pymysql.update(sql)
        except Exception as e:
            print(f"发生错误：{str(e)}")


def main():
    # 查询数据测试
    # result_pd2 = select_data()
    # print(result_pd2)
    # 清空表数据
    truncate_data()
    # 插入表数据
    insert_data()


if __name__ == '__main__':
    main()

