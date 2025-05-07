## PYTHON
## ******************************************************************** ##
## author: chenzhigao
## create time: 2025/04/08 15:46:07 GMT+08:00
## ******************************************************************** ##
# 导入依赖包
import warnings
import urllib3
import time
import textwrap
import json
import requests
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from yssdk.dli.sql_client import YsDliSQLCLient
from yssdk.common.alarm import Alarm
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
this_day = (datetime.now()).strftime("%Y%m%d")

# 检测sql
sql1 = f''' 
select 
    需求月份
    , 供给部门_合并
    , 商运或买手
    , 供应商id_合并
    , 供应商名称
    , round(商家采购金额,2) as 商家采购金额
    , concat(round(商家总断货占比 * 100,4),'%') as 商家总断货占比
    , concat(round(剩余未配 * 100,4),'%') as 剩余未配
    ,case 
        when 商家总断货占比 > 0.1 then '累计断货率大于10%'
        when 商家总断货占比 > 0.07 and 商家总断货占比 <= 0.1 then '累计断货率7%到10%'
    end as 优先级
from (
    select * from (
        select 
            需求月份
            ,供给部门_合并
            ,商运或买手
            ,供应商id_合并
            ,供应商名称
            ,订货单计划代采 + 前置仓发货 as 商家采购金额
            ,round((已断货+已退款+转抵扣+次品入库h360小时以外履约金额) / (订货单计划代采+前置仓发货),4) as 商家总断货占比
            ,round(1 - (
                ((订货单正常品已入库+前置仓发货) / (订货单计划代采+前置仓发货))
                +
                ((已断货+已退款+转抵扣+次品入库h360小时以外履约金额) / (订货单计划代采+前置仓发货))
            ),4) as 剩余未配
        from (
            select 
                需求月份
                ,供给部门_合并
                ,商运或买手
                ,供应商id_合并
                ,供应商名称
                ,sum(订货单计划代采) as 订货单计划代采
                ,sum(前置仓发货) as 前置仓发货
                ,sum(已断货) as 已断货
                ,sum(已退款) as 已退款
                ,sum(转抵扣) as 转抵扣
                ,sum(次品入库h360小时以外履约金额) as 次品入库h360小时以外履约金额
                ,sum(开单h168小时履约金额) as 开单h168小时履约金额
                ,sum(订货单正常品已入库) as 订货单正常品已入库
            from yishou_daily.temp_realtime_new_supply_allot_alarm
            where 供给部门_合并 = '1十三行-许愿' 
            and (
            to_char(需求日期,'yyyymmdd') between to_char(to_date1({this_day},'yyyymmdd'),'yyyymm01') 
            and 
                case 
                    when month(dateadd(to_date1({this_day},'yyyymmdd'),-1,'dd')) == month(to_date1({this_day},'yyyymmdd'))
                    then to_char(dateadd(to_date1({this_day},'yyyymmdd'),-1,'dd'),'yyyymmdd')
                    else '{this_day}'
                end
            )
            group by 1,2,3,4,5
        )
    )
    order by 商家采购金额 desc 
    limit 200
)
where 商家总断货占比 > 0.07
;
'''
sql2 = f''' 
select 
    a.需求周期
    ,a.供给部门_合并
    ,a.商运或买手
    ,a.供应商id_合并
    ,a.供应商名称
    ,round(a.商家采购金额,2) as 商家采购金额
    ,concat(round(a.商家7天履约率 * 100,4),'%') as 商家7天履约率
from (
    select * from (
        select 
            需求周期
            ,供给部门_合并
            ,商运或买手
            ,供应商id_合并
            ,供应商名称
            ,订货单计划代采 + 前置仓发货 as 商家采购金额
            ,round((开单h168小时履约金额+前置仓发货) / (订货单计划代采+前置仓发货),4) as 商家7天履约率
        from (
           select 
                concat(
                    to_char(to_date1({this_day},'yyyymmdd'),'yyyymm01'),
                    '-',
                    case 
                        when month(dateadd(to_date1({this_day},'yyyymmdd'),-7,'dd')) == month(to_date1({this_day},'yyyymmdd'))
                        then to_char(dateadd(to_date1({this_day},'yyyymmdd'),-7,'dd'),'yyyymmdd')
                        else {this_day}
                    end
                ) as 需求周期
                ,供给部门_合并
                ,商运或买手
                ,供应商id_合并
                ,供应商名称
                ,sum(订货单计划代采) as 订货单计划代采
                ,sum(前置仓发货) as 前置仓发货
                ,sum(已断货) as 已断货
                ,sum(已退款) as 已退款
                ,sum(转抵扣) as 转抵扣 
                ,sum(次品入库h360小时以外履约金额) as 次品入库h360小时以外履约金额
                ,sum(开单h168小时履约金额) as 开单h168小时履约金额
            from yishou_daily.temp_realtime_new_supply_allot_alarm
            where 供给部门_合并 = '1十三行-许愿' 
            and (
            to_char(需求日期,'yyyymmdd') between to_char(to_date1({this_day},'yyyymmdd'),'yyyymm01') 
            and 
                case 
                    when month(dateadd(to_date1({this_day},'yyyymmdd'),-7,'dd')) == month(to_date1({this_day},'yyyymmdd'))
                    then to_char(dateadd(to_date1({this_day},'yyyymmdd'),-7,'dd'),'yyyymmdd')
                    else {this_day}
                end
            )
            group by 1,2,3,4,5
        )
    )
    order by 商家采购金额 desc
    limit 200
) a
left join (
    select 
        供应商id_合并
    from (
        select * from (
            select 
                需求月份
                ,供给部门_合并
                ,商运或买手
                ,供应商id_合并
                ,供应商名称
                ,订货单计划代采 + 前置仓发货 as 商家采购金额
                ,round((已断货+已退款+转抵扣+次品入库h360小时以外履约金额) / (订货单计划代采+前置仓发货),4) as 商家总断货占比
            from (
                select 
                    需求月份
                    ,供给部门_合并
                    ,商运或买手
                    ,供应商id_合并
                    ,供应商名称
                    ,sum(订货单计划代采) as 订货单计划代采
                    ,sum(前置仓发货) as 前置仓发货
                    ,sum(已断货) as 已断货
                    ,sum(已退款) as 已退款
                    ,sum(转抵扣) as 转抵扣
                    ,sum(次品入库h360小时以外履约金额) as 次品入库h360小时以外履约金额
                    ,sum(开单h168小时履约金额) as 开单h168小时履约金额
                from yishou_daily.temp_realtime_new_supply_allot_alarm
                where 供给部门_合并 = '1十三行-许愿'
                and (
                to_char(需求日期,'yyyymmdd') between to_char(to_date1({this_day},'yyyymmdd'),'yyyymm01') 
                and 
                    case 
                        when month(dateadd(to_date1({this_day},'yyyymmdd'),-1,'dd')) == month(to_date1({this_day},'yyyymmdd'))
                        then to_char(dateadd(to_date1({this_day},'yyyymmdd'),-1,'dd'),'yyyymmdd')
                        else {this_day}
                    end
                )
                group by 1,2,3,4,5
            )
        )
        order by 商家采购金额 desc 
        limit 200
    )
    where 商家总断货占比 > 0.07
) b 
on a.供应商id_合并 = b.供应商id_合并
where a.商家7天履约率 < 0.85 and b.供应商id_合并 is null
;
'''

sql3 = f''' 
select 
    distinct a.nickname
    ,a.realname
    ,c.商运或买手 as flower_name
    ,a.user_id
from yishou_data.all_fmys_feishu_user_test a 
left join (
    select 
        供给部门
        ,花名
        ,`商运/买手` as 商运或买手
        , 组别
    from yishou_daily.ods_flower_name_list_excel
) b on a.nickname = b.花名
left join yishou_daily.temp_realtime_new_supply_allot_alarm c on c.商运或买手 = b.商运或买手
where b.花名 is not null and c.商运或买手 is not null
;
'''


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
                        "alt": {
                            "tag": "plain_text",
                            "content": "TOP200-断货风险商家"
                        },
                        "mode": "fit_horizontal",
                        "preview": True
                    },
                    {
                        "tag": "img",
                        "img_key": image_keys[1],
                        "alt": {
                            "tag": "plain_text",
                            "content": "TOP200-7天履约预警商家"
                        },
                        "mode": "fit_horizontal",
                        "preview": True
                    },
                    {
                        "tag": "hr"
                    },
                    {
                        "tag": "markdown",
                        "content": "上图相关业务:<at ids=\"" + duanhuo_flower_ids + "\"></at>",
                        "text_align": "left",
                        "text_size": "normal"
                    },
                    {
                        "tag": "markdown",
                        "content": "下图相关业务:<at ids=\"" + ly_flower_ids + "\"></at>",
                        "text_align": "left",
                        "text_size": "normal"
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


# 设置全局字体路径
plt.rcParams['font.sans-serif'] = ['SimHei']
# 解决负号显示异常
plt.rcParams['axes.unicode_minus'] = False


# 将数据转为表格图片
def made_tu_duanhuo(data_duanhuo):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(32, 20))  # 设置图片大小
    ax.axis('off')  # 隐藏坐标轴
    # 处理列名换行
    columns = [textwrap.fill(col, width=11) for col in data_duanhuo.columns]
    table = ax.table(
        cellText=data_duanhuo.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )

    print(f'列名:{columns}')
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(16)
    table.scale(1, 2.0)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.12)  # 设置单元格宽度
        cell.set_edgecolor('#dddddd')  # 设置边框颜色
    sum_jiankong_path = '/apps/data/project/tmp/ly_alarm_department_infos_denger_13hang_xuyuan_top200duanhuo.png'
    # 如果图片文件已存在，删除旧文件
    if os.path.exists(sum_jiankong_path):
        print('图片已存在，清除旧文件')
        os.remove(sum_jiankong_path)
    # 保存表格图片
    plt.savefig(sum_jiankong_path)


# 将数据转为表格图片
def made_tu_ly(data_ly):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(32, 20))  # 设置图片大小
    ax.axis('off')  # 隐藏坐标轴
    # 处理列名换行
    columns = [textwrap.fill(col, width=11) for col in data_ly.columns]
    table = ax.table(
        cellText=data_ly.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )

    print(f'列名:{columns}')
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(16)
    table.scale(1, 2.0)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.12)
        cell.set_edgecolor('#dddddd')
    sum_jiankong_path = '/apps/data/project/tmp/ly_alarm_department_infos_denger_13hang_xuyuan_top200ly.png'
    # 如果图片文件已存在，删除旧文件
    if os.path.exists(sum_jiankong_path):
        print('图片已存在，清除旧文件')
        os.remove(sum_jiankong_path)
    # 保存表格图片
    plt.savefig(sum_jiankong_path)


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


# 上传图片
def upload_img():
    url = "https://open.feishu.cn/open-apis/im/v1/images"
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    payload = {'image_type': 'message'}
    image_list = [
        'ly_alarm_department_infos_denger_13hang_xuyuan_top200duanhuo.png',
        'ly_alarm_department_infos_denger_13hang_xuyuan_top200ly.png'
    ]
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


if __name__ == '__main__':
    # 建立数据连接
    client = YsDliSQLCLient(queue='develop', database='yishou_daily')

    # 执行sql语句
    client.exec_sql(sql1)
    # 获取全部结果转为dataframe形式数据
    data_duanhuo = client.fetch_all_dataframe()
    # 将 NaN 替换成空字符串
    data_duanhuo = data_duanhuo.fillna('')

    # 执行sql语句
    client.exec_sql(sql2)
    # 获取全部结果转为dataframe形式数据
    data_ly = client.fetch_all_dataframe()
    data_ly_date = data_ly.iloc[0, 0]
    # 将 NaN 替换成空字符串
    data_ly = data_ly.fillna('')

    # 执行sql3
    client.exec_sql(sql3)
    data_flower = client.fetch_all_dataframe()

    # 合并匹配
    merged_df = pd.merge(data_duanhuo, data_flower, left_on='商运或买手', right_on='flower_name', how='inner')
    # 提取并转换为逗号分隔，去重
    duanhuo_flower_ids = ','.join(set(merged_df['user_id'].tolist()))

    # 合并匹配
    merged_df_2 = pd.merge(data_ly, data_flower, left_on='商运或买手', right_on='flower_name', how='inner')
    # 提取并转换为逗号分隔，去重
    ly_flower_ids = ','.join(set(merged_df_2['user_id'].tolist()))

    # # 合并输出
    # duanhuo_flower_ids_set = set(duanhuo_flower_ids.split(','))
    # ly_flower_ids_set = set(ly_flower_ids.split(','))
    # merged_flower_ids_set = duanhuo_flower_ids_set.union(ly_flower_ids_set)
    # merged_flower_ids = ','.join(merged_flower_ids_set)

    # 制作图片
    made_tu_ly(data_ly)
    made_tu_duanhuo(data_duanhuo)
    # 上传所有图片
    image_keys = upload_img()
    # 设置飞书告警总标题
    # 开启告警类，设置标题
    alarm = Alarm_XG(alarm_title='15天履约率预警')
    # 设置告警颜色
    alarm.set_color('green')
    # 设置发送飞书群
    alarm.set_alarm_field('当前时间点', this_hour)
    alarm.set_alarm_field("上图名称", "累计断货率top200商家")
    alarm.set_alarm_field("下图名称", f"{data_ly_date}累计7天配低于85%top200商家(剔除上图商家)")
    alarm.set_alarm_field("部门", "十三行-许愿")

    # 测试
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/294cf0df-3150-410e-b25d-6932f0cf38df')
    # alarm.send_feishu(alarm.build_alarm())
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/3bfbc233-5947-4762-9dac-9834485e6e6d')
    alarm.send_feishu(alarm.build_alarm())
    # alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/98a9e702-3e89-4b09-bd3a-9f82dd6d0d03')
    # alarm.send_feishu(alarm.build_alarm())
