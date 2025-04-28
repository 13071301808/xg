## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2025/04/08 15:46:07 GMT+08:00
## ******************************************************************** ##
# 导入依赖包
import urllib3
import textwrap
import requests
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.font_manager as fm
from yssdk.dli.sql_client import YsDliSQLCLient
from matplotlib.font_manager import *
from datetime import datetime, timedelta

# 全局配置参数
# 添加忽略网络安全提示
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
pd.set_option('display.colheader_justify', 'center')
pd.set_option('display.max_rows', None)  # 设置行数为无限制
pd.set_option('display.max_columns', 6)  # 设置列数为无限制
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', 60)  # 设置打印宽度(**重要**)

this_hour = (datetime.now()).strftime("%H")

# 检测sql
sql = f''' 
    select
        供给部门_合并
        ,订货单计划代采
        ,concat(round(昨日商家履约 * 100,4),'%') as 昨日商家履约
        ,concat(round(预估24小时履约率 * 100,4),'%') as 预估24小时履约率
        ,concat(round(预估环比 * 100,4),'%') as 预估环比
    from (
        select 
            供给部门_合并
            ,round(sum(订货单计划代采),2) as 订货单计划代采
            ,round((sum(前一日开单h24小时履约金额+前一日前置仓发货) / sum(前一日订货单计划代采+前一日前置仓发货)),4) as 昨日商家履约
            ,round((sum(开单h24小时履约金额+配送在途+前置仓发货) / sum(订货单计划代采+前置仓发货)),4) as 预估24小时履约率
            ,round(((sum(开单h24小时履约金额+配送在途+前置仓发货) / sum(订货单计划代采+前置仓发货)) - (sum(前一日开单h24小时履约金额+前一日前置仓发货) / sum(前一日订货单计划代采+前一日前置仓发货))),4) as 预估环比
        from yishou_daily.temp_realtime_order_allot_24h_department
        group by 1
        union 
        select 
            '合计' as 供给部门_合并
            ,round(sum(订货单计划代采),2) as 订货单计划代采
            ,round((sum(前一日开单h24小时履约金额+前一日前置仓发货) / sum(前一日订货单计划代采+前一日前置仓发货)),4) as 昨日商家履约
            ,round((sum(开单h24小时履约金额+配送在途+前置仓发货) / sum(订货单计划代采+前置仓发货)),4) as 预估24小时履约率
            ,round(((sum(开单h24小时履约金额+配送在途+前置仓发货) / sum(订货单计划代采+前置仓发货)) - (sum(前一日开单h24小时履约金额+前一日前置仓发货) / sum(前一日订货单计划代采+前一日前置仓发货))),4) as 预估环比
        from yishou_daily.temp_realtime_order_allot_24h_department
    )
    order by 订货单计划代采 desc
'''

# # 设置全局字体路径
# font_path = '/usr/share/fonts/wqy-microhei/wqy-microhei.ttc'  # 替换为你的字体文件路径
# # 加载字体
# custom_font = fm.FontProperties(fname=font_path)
# 设置全局字体
plt.rcParams['font.family'] = ['WenQuanYi Micro Hei']

# 将数据转为表格图片
def made_tu(data):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(18, 8))  # 设置图片大小
    ax.axis('off')  # 隐藏坐标轴
    # 处理列名换行
    columns = [textwrap.fill(col, width=11) for col in data.columns]
    table = ax.table(
        cellText=data.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )

    print(f'列名:{columns}')
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(17)
    table.scale(1, 3.0)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.15)  # 设置单元格宽度
        cell.set_edgecolor('#dddddd')  # 设置边框颜色
    sum_jiankong_path = 'F://data/picture/ly_alarm_department_new.png'
    # 如果图片文件已存在，删除旧文件
    if os.path.exists(sum_jiankong_path):
        print('图片已存在，清除旧文件')
        os.remove(sum_jiankong_path)
    # 保存表格图片
    plt.savefig(sum_jiankong_path)


# 上传图片
def upload_img():
    url = "https://open.feishu.cn/open-apis/im/v1/images"
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    payload = {'image_type': 'message'}
    image_list = ['ly_alarm_department_new.png']
    image_keys = []
    for img in image_list:
        files = [
            ('image',
             (img, open(f'/apps/data/project/tmp/{img}', 'rb'), 'application/json'))
        ]
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response_data = json.loads(response.text)
        image_key = response_data['data']['image_key']
        print(image_key)
        image_keys.append(image_key)
    return image_keys


# 自动获取最新的token
def get_authorization():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": "cli_a46f1952bb79d00c",
        "app_secret": "s9xm9RJ7KAdxlKMIzBZ0ofWYpsdG3Wao",
        "chat_id": ""
    })
    headers = {
        'Content-Type': 'application/json;charset=utf-8'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response_data = json.loads(response.text)
    Authorization = response_data['tenant_access_token']
    return Authorization


# 上传文件
def upload_file(file_path):
    url = "https://open.feishu.cn/open-apis/im/v1/files"
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    payload = {'file_type': 'xls', 'file_name': '履约播报_部门告警统计.xls'}
    files = [
        ('file', ('stage_infos.xlsx', open(f'{file_path}', 'rb'), 'application/json'))
    ]
    response = requests.request("POST", url, headers=headers, data=payload, files=files)
    response_data = json.loads(response.text)
    file_key = response_data['data']['file_key']
    print(file_key)
    return file_key


# 下载文件
def upload_sumfile(file_key):
    url = f"https://open.feishu.cn/open-apis/im/v1/files/{file_key}"
    payload = ''
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    resp_file = response.text
    return resp_file


def send(this_file_key):
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    params = {"receive_id_type": "chat_id"}
    msgcontent = {
        "file_key": this_file_key,
    }
    req = {
        "receive_id": "oc_6e051973a20aa4c7c745a5e908dc5808",
        "msg_type": "file",
        "content": json.dumps(msgcontent)
    }
    payload = json.dumps(req)
    headers = {
        'Authorization': 'Bearer ' + get_authorization(),
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, params=params, headers=headers, data=payload)
    print("文件发送：")
    print(response.headers['X-Tt-Logid'])  # for debug or oncall
    print(response.content)  # Print Response


class Alarm_XG:
    def __init__(self, alarm_title):
        self.is_short = False
        self.tag = "lark_md"
        self.alarm_title = alarm_title
        self.headers = {
            'Authorization': 'Bearer ' + get_authorization(),
            'Content-Type': 'application/json'
        }
        self.alarm_content = []
        self.alarm_url = 'https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df'
        self.color = "red"

    # 告警json数据汇总
    def generate_alarm_message(self, alarm_field_message):
        return {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "elements": [
                    {
                        "tag": "img",
                        "img_key": image_keys[0],
                        # "alt": {
                        #     "tag": "plain_text",
                        #     "content": "排序规则: 以T-1日应开单金额进行倒序"
                        # },
                        "mode": "fit_horizontal",
                        "preview": True
                    },
                    {
                        "tag": "hr"
                    },
                    {
                        "fields": alarm_field_message,
                        "tag": "div"
                    }
                ],
                "header": {
                    "template": self.color,
                    "title": {
                        "content": self.alarm_title,
                        "tag": "plain_text"
                    }
                }
            }
        }

    # 设置飞书告警url
    def set_alarm_url(self, url):
        self.alarm_url = url

    # 设置飞书模板颜色
    def set_color(self, color):
        self.color = color

    # 生成告警字段的信息
    def get_alarm_field_info(self, field_name, field_value):
        field_info = {
            "is_short": self.is_short,
            "text": {
                "content": "**%s**: %s" % (field_name, field_value),
                "tag": self.tag
            }
        }
        return field_info

    # 设置告警字段的信息
    def set_alarm_field(self, field_name, field_value):
        field_info = self.get_alarm_field_info(field_name, field_value)
        self.alarm_content.append(field_info)

    # 生成告警信息
    def build_alarm(self):
        return self.generate_alarm_message(self.alarm_content)

    # 发送到飞书
    def send_feishu(self, alarm_message):
        print(str(alarm_message))
        response = requests.post(
            url=self.alarm_url,
            data=json.dumps(alarm_message),
            headers=self.headers,
            verify=False
        )
        return response


if __name__ == '__main__':
    # 建立数据连接
    client = YsDliSQLCLient(queue='develop', database='yishou_daily')
    # 执行sql语句
    client.exec_sql(sql)
    # 获取全部结果转为dataframe形式数据
    result = client.fetch_all_dataframe()
    # 将 NaN 替换成空字符串
    result = result.fillna('')
    # 制作图片
    made_tu(result)
    # 上传所有图片
    image_keys = upload_img()
    # 设置飞书告警总标题
    # 开启告警类，设置标题
    alarm = Alarm_XG(alarm_title='履约_部门')
    # 设置告警颜色
    alarm.set_color('green')
    # 设置发送飞书群
    alarm.set_alarm_field('当前时间点', this_hour)
    alarm.set_alarm_field("名称", "预估今日开单市场达成24配情况")
    # 测试
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df')
    alarm.send_feishu(alarm.build_alarm())
    # # 沙河南
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/9e00ee7b-2aa2-45fd-badd-a9a545cfed18')
    # alarm.send_feishu(alarm.build_alarm())
    # # 十三行许愿
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/98a9e702-3e89-4b09-bd3a-9f82dd6d0d03')
    # alarm.send_feishu(alarm.build_alarm())
    # # 十三行勇强
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/0be68501-ffc8-40e5-8e98-a4ee10763703')
    # alarm.send_feishu(alarm.build_alarm())
    # # 沙河金马
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/39c08ddb-0421-47f5-a507-b5dfcdbad6b4')
    # alarm.send_feishu(alarm.build_alarm())
    # # 深圳南油
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/8e87a0f0-7f8e-4f31-8ae0-a2d9c713fefd')
    # alarm.send_feishu(alarm.build_alarm())