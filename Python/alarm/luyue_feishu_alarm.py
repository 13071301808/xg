import warnings
import urllib3
import time
import textwrap
import json
import requests
import os
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timedelta
from yssdk.dli.sql_client import YsDliSQLCLient

# 全局配置参数
# 设置中文字体
plt.rcParams['font.sans-serif'] = ['WenQuanYi Micro Hei']
plt.rcParams['axes.unicode_minus'] = False
# 添加忽略网络安全提示
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=RuntimeWarning)
# 获取日期
today = datetime.now()
if today.hour % 2 == 0:
    today_hour = (today - timedelta(hours=1)).hour
else:
    today_hour = today.hour
today_time = today.strftime("%Y%m%d")
last_22day_ago = (datetime.now() - timedelta(days=22)).strftime("%Y%m%d")
last_16day_ago = (datetime.now() - timedelta(days=16)).strftime("%Y%m%d")
client = YsDliSQLCLient(queue='analyst')

# today_hour = '13'
# 各时间点下的负责人数据
sql = f'''
    SELECT    
        日期
        ,订单负责人 
        ,时间点
        ,城市
        ,case 
            when 城市 = '深圳' then '广州'
            when 城市 = '青岛' then '杭州'
            else 城市 
        end as 新城市
        ,`在途库存超7D` as `非异常在途超7天`
        ,`异地在途库存超7D` as `异地在途超7天`
        ,`欠货节点_已支付_超60D_排单单` as  `欠货节点超60天(排单已支付)`
        ,`欠货节点_已支付_超30D_实数单` as `欠货节点超30天(实数单已支付)`
        ,`欠货节点_未支付_超60D_排单单` as `欠货节点超60天(排单未支付)`
        ,`抵扣金额超30D` as `抵扣超30天`
        ,`待开单_排单单商家_超30D` as `待开单超30天(排单)`
        ,`代采订单支付节点_未支付_超60D` as `支付节点超60天`
        ,`待开单_实数单商家_超30D` as `待开单超30天(实数单)`
    FROM      yishou_daily.finebi_purchase_owner_20240219
    where 日期={today_time} and 时间点 = {today_hour};
'''
# 各节点明细数据
sql1 = f'''
    with three_six as--近7天合计15天配
    (
        SELECT 
            supply_id, 
            COALESCE(SUM(create_amount), 0) AS 十五天配分母,
            COALESCE(SUM(allot_amount_360h), 0) AS 十五天配分子
        FROM yishou_daily.finebi_supply_chain_sp_purchase_allot_dt 
        WHERE dt between {last_22day_ago} and {last_16day_ago}
        GROUP BY supply_id
    )
    SELECT 
        '异地在途超7天' as 节点名称
        ,onorder_time 在途持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no  货号
        ,goods_kh  款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt as 日期
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_cg_transit_over72h_dt
    where substr(dt,1,8) = {today_time} and cg_owe_tag = '异地' and onorder_time > 7
    UNION ALL
    SELECT 
        '非异常在途超7天' 节点名称
        ,onorder_time 在途持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no  货号
        ,goods_kh  款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt 日期
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_cg_transit_over72h_dt
    where substr(dt,1,8) = {today_time} and cg_owe_tag = '在途库存' and onorder_time > 7
    UNION ALL
    -- 欠货节点超60天(排单已支付)
    SELECT  
        '欠货节点超60天(排单已支付)' 节点名称
        ,onorder_time 在途持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no  货号
        ,goods_kh  款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_cg_wcg_node_dt
    where  status = '已支付' and cg_ld_fs = '排单单'
    and substr(dt,1,8) = {today_time} and onorder_time > 60
    UNION ALL
    -- 欠货节点超30天(实数单已支付)
    SELECT  
        '欠货节点超30天(实数单已支付)' 节点名称
        ,onorder_time 在途持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no  货号
        ,goods_kh  款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_cg_wcg_node_dt
    where  status = '已支付' and cg_ld_fs = '实数单'
    and substr(dt,1,8) = {today_time} and onorder_time > 30
    UNION ALL
     -- 欠货节点超60天(排单未支付)
    SELECT 
        '欠货节点超60天(排单未支付)' 节点名称
        ,onorder_time 持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no  货号
        ,goods_kh  款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_cg_wcg_node_dt
    where  status = '未支付'
    and substr(dt,1,8) = {today_time} and onorder_time > 60
    UNION ALL
    -- 抵扣金额超30D
    SELECT 
        '抵扣超30天'节点名称
        ,onorder_time 持续时长
        ,onorder_time_period 持续时长区间
        ,to_char(goods_no)  货号
        ,null 款号
        ,co_val  颜色
        ,si_val  尺码
        ,big_market  大市场
        ,supply_id  `供应商id`
        ,supply_name  供应商名字_合并
        ,city  城市
        ,pg_name  采购组
        ,pg_member_name  订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    from yishou_daily.finebi_wcg_ded_over_15d_dt
    where substr(dt,1,8) = {today_time} and onorder_time > 30
        and cg_type = '常规代采' 
        and owe_type = '代采欠货' 
    UNION ALL
     -- 待开单_排单单商家_超30D
    select 
        节点名称
        ,持续时长
        ,持续时长区间
        ,货号
        ,null 款号
        ,null 颜色
        ,null 尺码
        ,市场
        ,供应商id
        ,供应商名字_合并
        ,城市
        ,采购组名称
        ,采购员
        ,dt
        ,跑数时间
        ,时间点
    from (
        select a.*
        ,c.十五天配分母,c.十五天配分子,if(coalesce(c.十五天配分母,0) != 0 ,c.十五天配分子/c.十五天配分母,0) 十五天配
        --,case when c.十五天配分子/c.十五天配分母 < 0.914 then 待开单 else 0 end as 待开单_剔除15天配
        from (
            SELECT  
                '待开单超30天(排单)'节点名称
                ,onorder_time 持续时长
                ,onorder_time_period 持续时长区间
                ,planid `采购计划id`
                ,second_market_name 市场
                ,supply_id `供应商id`
                ,to_char(goods_no) `货号`
                ,supply_name 供应商名字_合并
                ,city 城市
                ,pg_name 采购组名称
                ,pg_member_name 采购员
                ,dt
                ,substr(dt,1,8) 跑数时间
                ,substr(dt,9,2) 时间点-- hh 
            FROM yishou_daily.finebi_dkd_supply_over15d_dt
            where billing_type ='是'
            and substr(dt,1,8) = {today_time} and onorder_time > 30
        )a 
        left join three_six c on a.`供应商id` = c.supply_id
    )where 十五天配 < 0.914
    UNION ALL
    SELECT 
        '待开单超30天(实数单)' 节点名称
        ,onorder_time 持续时长
        ,onorder_time_period 持续时长区间
        ,to_char(goods_no) 货号
        ,null 款号
        ,null 颜色
        ,null 尺码
        ,second_market_name 市场
        ,supply_id `供应商id`
        ,supply_name 供应商名字_合并
        ,city 城市
        ,pg_name 采购组名称
        ,pg_member_name 订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh 
    FROM yishou_daily.finebi_dkd_supply_over15d_dt
    where billing_type ='否'
    and onorder_time > 30
    and substr(dt,1,8) = {today_time}
     -- 支付节点超60天
    UNION ALL
    SELECT 
        '支付节点超60天' 节点名称
        ,onorder_time 持续时长
        ,onorder_time_period 持续时长区间
        ,goods_no 货号
        ,null 款号
        ,null 颜色
        ,null 尺码
        ,second_market_name 市场
        ,supply_id `供应商id`
        ,supply_name 供应商名字_合并
        ,city 城市
        ,pg_name 采购组
        ,pg_member_name 订单负责人
        ,dt
        ,substr(dt,1,8) 跑数时间
        ,substr(dt,9,2) 时间点-- hh  
    FROM yishou_daily.finebi_non_payment_over15d_dt
    where substr(dt,1,8) = {today_time} and onorder_time > 60
'''


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
                        "alt": {
                            "tag": "plain_text",
                            "content": "日监控汇总(负责人维度)"
                        },
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


# 将数据转为表格图片
def made_tu(data):
    # 创建表格图片
    fig, ax = plt.subplots(figsize=(24, 12))  # 设置图片大小
    ax.axis('off')  # 隐藏坐标轴
    # 处理列名换行
    columns = [textwrap.fill(col, width=8) for col in data.columns]
    table = ax.table(
        cellText=data.values, colLabels=columns,
        loc='center', cellLoc='center',
        colColours=['lightgray'] * len(columns)
    )
    # 调整表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(15)
    table.scale(1, 3.0)
    # 调整单元格宽度
    for key, cell in table.get_celld().items():
        cell.set_width(0.085)  # 设置单元格宽度
        cell.set_edgecolor('#dddddd')  # 设置边框颜色

    # 添加标题
    plt.title("日监控汇总", fontsize=24)
    sum_jiankong_path = '/data/project/test/performance/table.png'
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
    image_list = ['table.png']
    image_keys = []
    for img in image_list:
        files = [
            ('image',
             (img, open(f'/data/project/test/performance/{img}', 'rb'), 'application/json'))
        ]
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        response_data = json.loads(response.text)
        image_key = response_data['data']['image_key']
        print(image_key)
        image_keys.append(image_key)
    return image_keys


# 上传文件
def upload_file(file_path):
    url = "https://open.feishu.cn/open-apis/im/v1/files"
    headers = {
        'Authorization': 'Bearer ' + get_authorization()
    }
    payload = {'file_type': 'xls', 'file_name': '履约中台明细.xls'}
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


if __name__ == '__main__':
    print('当前运行时间（小时）:', today_hour)
    # 执行sql语句
    client.exec_sql(sql)
    # 获取全部结果转为dataframe形式数据
    data = client.fetch_all_dataframe()
    # 将 NaN 替换成空字符串
    data = data.fillna('')
    made_tu(data)
    # 上传所有图片
    image_keys = upload_img()

    # 各节点明细数据
    client.exec_sql(sql1)
    # 获取全部结果转为dataframe形式数据
    data_infos = client.fetch_all_dataframe()
    file_path = '/data/project/test/performance/stage_infos.xlsx'
    # 转为xls文件存储到服务器
    data_infos_excel = data_infos.to_excel(file_path, index=False, encoding='utf8')
    # 将文件上传到飞书
    file_key = upload_file(file_path)
    # 实现下载
    resp_file = upload_sumfile(file_key)

    # 开启告警类，设置标题
    alarm = Alarm_XG(alarm_title='履约中台节点明细监控')
    # 设置告警颜色
    alarm.set_color('green')
    # 添加文字提示
    alarm.set_alarm_field('当前时间点', today_hour)
    alarm.set_alarm_field('注', '以上图片为日监控汇总(负责人维度)')
    # 测试url
    alarm.set_alarm_url('https://open.feishu.cn/open-apis/bot/v2/hook/4f218c14-5e50-4f33-8c57-adfec59e09c7')
    alarm.send_feishu(alarm.build_alarm())
    send(file_key)

# 测试url: https://open.feishu.cn/open-apis/bot/v2/hook/18aefde7-f9b5-48cc-a6b3-e23f259060e3